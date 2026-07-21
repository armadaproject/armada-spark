#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys

from pyspark.sql import SparkSession

JDBC_PASSWORD_ENV = "ARMADA_JDBC_PASSWORD"
NUM_PARTITIONS = 8


def resolve_password(argv_password):
    if argv_password:
        return argv_password
    return os.environ.get(JDBC_PASSWORD_ENV, "")


def jdbc_writer(df, url, user, password, table, driver):
    (
        df.write.mode("overwrite")
        .format("jdbc")
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", table)
        .option("driver", driver)
        .save()
    )


def jdbc_reader(spark, url, user, password, table, driver):
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", table)
        .option("driver", driver)
        .load()
    )


def action_write_read(spark, url, user, password, table, rows, driver):
    df = spark.range(rows).selectExpr("id", "CAST(id AS STRING) AS payload")
    jdbc_writer(df, url, user, password, table, driver)

    read_back = jdbc_reader(spark, url, user, password, table, driver).count()
    print(f"ARMADA_TEST_JDBC_ROWS={read_back}")
    if read_back != rows:
        print(
            f"ERROR: wrote {rows} rows but read back {read_back}", file=sys.stderr
        )
        spark.stop()
        sys.exit(1)
    spark.stop()


def action_partitioned_read(spark, url, user, password, table, rows, driver):
    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", table)
        .option("driver", driver)
        .option("partitionColumn", "id")
        .option("lowerBound", 0)
        .option("upperBound", rows)
        .option("numPartitions", NUM_PARTITIONS)
        .load()
    )
    count = df.count()
    print(f"ARMADA_TEST_JDBC_PARTITION_ROWS={count}")
    if count != rows:
        print(
            f"ERROR: partitioned read expected {rows} rows but got {count}",
            file=sys.stderr,
        )
        spark.stop()
        sys.exit(1)
    print("ARMADA_TEST_JDBC_PARTITIONS_OK=true")
    spark.stop()


def action_drop(spark, url, user, password, table, driver):
    try:
        jvm = spark._sc._jvm
        jvm.java.lang.Class.forName(driver)
        conn = jvm.java.sql.DriverManager.getConnection(url, user, password)
        try:
            stmt = conn.createStatement()
            stmt.execute("DROP TABLE IF EXISTS " + table)
            stmt.close()
        finally:
            conn.close()
        print("ARMADA_TEST_JDBC_DROPPED=true")
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: drop table {table} failed: {exc}", file=sys.stderr)
        spark.stop()
        sys.exit(1)
    spark.stop()


def main():
    if len(sys.argv) < 6:
        print(
            "ERROR: usage: db_roundtrip.py <action> <jdbc_url> <user> "
            "<password> <table> [rows] [driver_class]",
            file=sys.stderr,
        )
        sys.exit(2)

    action = sys.argv[1]
    url = sys.argv[2]
    user = sys.argv[3]
    password = resolve_password(sys.argv[4])
    table = sys.argv[5]
    rows = int(sys.argv[6]) if len(sys.argv) > 6 else 100_000
    driver = sys.argv[7] if len(sys.argv) > 7 else "org.postgresql.Driver"

    spark = SparkSession.builder.appName(f"armada-jdbc-{action}").getOrCreate()

    if action == "write_read":
        action_write_read(spark, url, user, password, table, rows, driver)
    elif action == "partitioned_read":
        action_partitioned_read(spark, url, user, password, table, rows, driver)
    elif action == "drop":
        action_drop(spark, url, user, password, table, driver)
    else:
        print(
            f"ERROR: unknown action '{action}' "
            "(expected write_read|partitioned_read|drop)",
            file=sys.stderr,
        )
        spark.stop()
        sys.exit(2)


if __name__ == "__main__":
    main()
