#!/bin/bash

source "$(dirname "$0")/../../lib/common.sh"

MODE_ARGS=("$@")

if require_infra db JDBC_URL; then
    print_summary
    exit $?
fi

CLUSTER_MODE=false
is_cluster_mode "${MODE_ARGS[@]+"${MODE_ARGS[@]}"}" && CLUSTER_MODE=true

# secretKeyRef delivers the password to pods as $ARMADA_JDBC_PASSWORD, but a
# client-mode driver runs locally (not as a pod) and never receives it, so its
# own JDBC connection would authenticate with an empty password. secretKeyRef is
# therefore cluster-mode only; skip rather than fail with a confusing auth error.
if [ -n "${JDBC_SECRET_KEY:-}" ] && ! $CLUSTER_MODE; then
    skip "db (JDBC_SECRET_KEY set in client mode; secretKeyRef reaches pods only, not the local driver; use -M cluster or the argv JDBC_PASSWORD)"
    print_summary
    exit $?
fi

USER_ARG="${JDBC_USER:-}"
DRIVER="${JDBC_DRIVER:-org.postgresql.Driver}"
TABLE="armada_spark_test_$$"
ROWS="${JDBC_ROWS:-100000}"
JOB="/opt/spark/extraFiles/jobs/db/db_roundtrip.py"

if [ -n "${JDBC_SECRET_KEY:-}" ]; then
    # Cluster mode only (guarded above): the password arrives in pods via
    # $ARMADA_JDBC_PASSWORD (secretKeyRef), not on argv, so pass an empty
    # password argument.
    PASS_ARG=""
    log "password via secretKeyRef env ARMADA_JDBC_PASSWORD (secret $JDBC_SECRET_KEY)"
else
    PASS_ARG="${JDBC_PASSWORD:-}"
fi

log "jdbc url: $JDBC_URL"
log "table=$TABLE rows=$ROWS driver=$DRIVER"

assert_success() {
    if $CLUSTER_MODE; then
        if driver_succeeded; then
            pass "$3 (driver job SUCCEEDED; sentinel not observable in cluster mode)"
        else
            fail "$2" "submission exited 0 but no 'Driver job ... SUCCEEDED' status seen"
            dump_tail 30
        fi
    else
        if output_matches "$1"; then
            pass "$3"
        else
            fail "$2" "submission succeeded but sentinel ($1) missing/mismatched"
            dump_tail 30
        fi
    fi
}

verify_db_rowcount() {
    local expect="$1" table="$2" name="$3"
    [ -n "${DB_VERIFY_USER:-}" ] || { log "DB-side verify skipped (DB_VERIFY_USER unset)"; return 0; }
    command -v java >/dev/null 2>&1 || { log "DB-side verify skipped (no java on host)"; return 0; }
    local jar="${DB_VERIFY_DRIVER_JAR:-}"
    if [ -z "$jar" ]; then
        # Auto-discovery only knows the PostgreSQL driver name; for any other DB
        # set DB_VERIFY_DRIVER_JAR explicitly.
        jar="$(ls "$SCRIPTS_DIR/../extraJars/"postgresql-*.jar 2>/dev/null | head -1)"
    fi
    [ -n "$jar" ] && [ -f "$jar" ] || { log "DB-side verify skipped (no driver jar; set DB_VERIFY_DRIVER_JAR)"; return 0; }

    local work; work="$(mktemp -d)"
    cat > "$work/DbVerify.java" <<'JAVA'
import java.sql.*;
public class DbVerify {
  public static void main(String[] a) throws Exception {
    Class.forName(System.getenv("V_DRIVER"));
    try (Connection c = DriverManager.getConnection(
             System.getenv("V_URL"), System.getenv("V_USER"), System.getenv("V_PASS"));
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("SELECT count(*) FROM " + System.getenv("V_TABLE"))) {
      r.next();
      System.out.println("DBVERIFY_COUNT=" + r.getLong(1));
    }
  }
}
JAVA
    if ! javac -d "$work" "$work/DbVerify.java" 2>"$work/err"; then
        log "DB-side verify skipped (javac failed: $(head -1 "$work/err"))"; rm -rf "$work"; return 0
    fi
    local out
    out="$(
        export V_URL="$JDBC_URL" V_USER="$DB_VERIFY_USER" V_PASS="${DB_VERIFY_PASSWORD:-}" \
               V_TABLE="$table" V_DRIVER="$DRIVER"
        ${TIMEOUT_BIN:+$TIMEOUT_BIN 60} java -cp "$work:$jar" DbVerify 2>"$work/err"
    )"
    # Pull the sentinel out of stdout rather than assuming it is the only line.
    local got; got="$(printf '%s\n' "$out" | grep -o 'DBVERIFY_COUNT=[0-9]*' | head -1)"
    got="${got#DBVERIFY_COUNT=}"
    rm -rf "$work"
    if [ -z "$got" ]; then
        fail "$name" "DB-side verify could not read $table (see err)"
        return 1
    fi
    if [ "$got" = "$expect" ]; then
        pass "DB-side verify: $table holds $got rows (matches expected $expect)"
    else
        fail "$name" "DB-side verify: $table holds $got rows, expected $expect"
    fi
}

start_case "J1: write + read-back ($ROWS rows)"
if run_submit "${MODE_ARGS[@]+"${MODE_ARGS[@]}"}" -P "$JOB" write_read "$JDBC_URL" "$USER_ARG" "$PASS_ARG" "$TABLE" "$ROWS" "$DRIVER"; then
    assert_success "ARMADA_TEST_JDBC_ROWS=$ROWS" "J1" "J1 wrote and read back $ROWS rows"
    verify_db_rowcount "$ROWS" "$TABLE" "J1"
else
    if [ "$LAST_RC" -eq 124 ]; then
        fail "J1" "write/read job HUNG until timeout (rc=124)"
    else
        fail "J1" "write/read job failed (rc=$LAST_RC)"
    fi
    dump_tail 30
fi

start_case "J2: partitioned read (concurrent executor connections)"
if run_submit "${MODE_ARGS[@]+"${MODE_ARGS[@]}"}" -P "$JOB" partitioned_read "$JDBC_URL" "$USER_ARG" "$PASS_ARG" "$TABLE" "$ROWS" "$DRIVER"; then
    assert_success "ARMADA_TEST_JDBC_PARTITIONS_OK=true" "J2" \
        "J2 partitioned read returned matching rows from concurrent connections"
else
    if [ "$LAST_RC" -eq 124 ]; then
        fail "J2" "partitioned read HUNG until timeout (rc=124)"
    else
        fail "J2" "partitioned read failed (rc=$LAST_RC)"
    fi
    dump_tail 30
fi

start_case "J3: bad credentials fails cleanly"
if SUBMIT_TIMEOUT="${J3_TIMEOUT:-180}" \
    run_submit "${MODE_ARGS[@]+"${MODE_ARGS[@]}"}" -P "$JOB" write_read "$JDBC_URL" "$USER_ARG" "armada-bogus-password-$$" "$TABLE" "$ROWS" "$DRIVER"; then
    fail "J3" "write with bad credentials unexpectedly succeeded"
    dump_tail 30
else
    if [ "$LAST_RC" -eq 124 ]; then
        fail "J3" "write with bad credentials HUNG until timeout (rc=124) instead of failing fast"
        dump_tail 30
    elif output_matches "auth|password|permission denied|28P01|FATAL|authentication"; then
        pass "J3 bad credentials failed fast with an auth error (rc=$LAST_RC)"
    else
        # J1/J2 already proved connectivity, the driver jar, and TLS with good
        # credentials, so a J3 failure here should be auth-related. An unmatched
        # signature means an unrelated failure (or a DB whose auth-error text
        # needs adding to the pattern above); do not count it as a pass.
        fail "J3" "bad-credential write failed for an unrecognized reason, not a known auth error (rc=$LAST_RC)"
        dump_tail 30
    fi
fi

start_case "J4: cleanup (drop table)"
if run_submit "${MODE_ARGS[@]+"${MODE_ARGS[@]}"}" -P "$JOB" drop "$JDBC_URL" "$USER_ARG" "$PASS_ARG" "$TABLE" "$ROWS" "$DRIVER"; then
    assert_success "ARMADA_TEST_JDBC_DROPPED=true" "J4" "J4 dropped table $TABLE"
else
    if [ "$LAST_RC" -eq 124 ]; then
        fail "J4" "drop job HUNG until timeout (rc=124)"
    else
        fail "J4" "drop job failed (rc=$LAST_RC)"
    fi
    dump_tail 30
fi

print_summary
