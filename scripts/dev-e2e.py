#!/usr/bin/env python3

import os
import sys
import subprocess
import platform
import urllib.request
import tarfile
import time
import re
import shutil
import json

# Setup base directories
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, ".."))
AOREPO = 'https://github.com/armadaproject/armada-operator.git'
AOHOME = os.path.abspath(os.path.join(PROJECT_ROOT, "..", "armada-operator"))
ARMADACTL_VERSION = '0.20.23'
ARMADACTL_PATH = os.path.join(SCRIPTS_DIR, 'armadactl')

# Colors for terminal output
GREEN = '\033[0;32m'
RED = '\033[0;31m'
NC = '\033[0m'

def log(msg):
    print(f"{GREEN}{msg}{NC}")

def err(msg):
    print(f"{RED}{msg}{NC}", file=sys.stderr)

def log_group(msg):
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::group::{msg}")
    print(msg)

def end_log_group():
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print("::endgroup::")

def source_env_script(script_path):
    """Sources a bash script and loads exported variables into the Python environment."""
    if not os.path.exists(script_path):
        return
    command = f"source {script_path} && env"
    try:
        proc = subprocess.run(['bash', '-c', command], capture_output=True, text=True, check=True)
        for line in proc.stdout.splitlines():
            if '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value
    except subprocess.CalledProcessError as e:
        err(f"Failed to source {script_path}: {e}")
        sys.exit(1)

def run_cmd(cmd, **kwargs):
    """Helper to run a shell command."""
    try:
        return subprocess.run(cmd, check=True, **kwargs)
    except subprocess.CalledProcessError as e:
        err(f"Command '{' '.join(cmd)}' failed with exit code {e.returncode}")
        sys.exit(e.returncode)

def fetch_armadactl():
    dl_url = 'https://github.com/armadaproject/armada/releases/download'
    sys_os = platform.system()
    machine = platform.machine()

    if sys_os == 'Darwin':
        dl_os = 'darwin'
        dl_arch = 'all'
    elif sys_os == 'Linux':
        dl_os = 'linux'
        if machine in ('aarch64', 'arm64'):
            dl_arch = 'arm64'
        elif machine == 'x86_64':
            dl_arch = 'amd64'
        else:
            err(f"fetch_armadactl(): sorry, architecture {machine} not supported; exiting now")
            sys.exit(1)
    else:
        err(f"fetch_armadactl(): sorry, operating system {sys_os} not supported; exiting now")
        sys.exit(1)

    dl_file = f"armadactl_{ARMADACTL_VERSION}_{dl_os}_{dl_arch}.tar.gz"
    full_url = f"{dl_url}/v{ARMADACTL_VERSION}/{dl_file}"

    log_group("Fetching armadactl")
    try:
        tar_path = os.path.join(SCRIPTS_DIR, dl_file)
        urllib.request.urlretrieve(full_url, tar_path)
        with tarfile.open(tar_path, "r:gz") as tar:
            # Extract specifically the armadactl binary
            for member in tar.getmembers():
                if member.name.endswith("armadactl"):
                    member.name = "armadactl" # flatten path
                    tar.extract(member, path=SCRIPTS_DIR)
        os.remove(tar_path)
        os.chmod(ARMADACTL_PATH, 0o755)
    except Exception as e:
        err(f"Failed to download and extract armadactl: {e}")
        sys.exit(1)
    finally:
        end_log_group()

def armadactl_retry(*args):
    cmd = [ARMADACTL_PATH] + list(args)
    for attempt in range(1, 11):
        proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if proc.returncode == 0:
            return True
        time.sleep(5)
    err(f"Running \"{' '.join(cmd)}\" failed after {attempt} attempts")
    return False


def start_armada():
    log_group("Using armada-operator to start Armada; this may take up to 5 minutes")
    
    if os.path.isdir(AOHOME):
        print(f"Using existing armada-operator repo at {os.path.realpath(AOHOME)}")
    else:
        if subprocess.run(['git', 'clone', AOREPO, AOHOME]).returncode != 0:
            err("There was a problem cloning the armada-operator repo; exiting now")
            sys.exit(1)
        
    log("Patching armada-operator")
    patch_file = os.path.join(PROJECT_ROOT, 'e2e', 'armada-operator.patch')
    with open(patch_file, 'r') as f:
        if subprocess.run(['patch', '-p1', '-d', f"{AOHOME}/"], stdin=f).returncode != 0:
            err(f"There was an error patching the repo copy {AOHOME}")
            sys.exit(1)

    print("Running 'make kind-all' to install and start Armada; this may take up to 6 minutes")
    make_proc = subprocess.run(['make', 'kind-all'], cwd=AOHOME, capture_output=True, text=True)
    with open("armada-start.txt", "w") as f:
        f.write(make_proc.stdout)
        f.write(make_proc.stderr)
    
    if make_proc.returncode != 0:
        print(make_proc.stdout)
        print(make_proc.stderr)
        print("")
        err("There was a problem starting Armada; exiting now")
        sys.exit(1)

    print("Extracting TLS client certificate files from Kind cluster")
    extract_cert_script = os.path.join(PROJECT_ROOT, 'e2e', 'extract-kind-cert.sh')
    if subprocess.run([extract_cert_script]).returncode != 0:
        err("There was a problem extracting the certificates")
        sys.exit(1)
        
    end_log_group()

def check_image(image_name):
    return subprocess.run(['docker', 'image', 'inspect', image_name],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0

def pin_single_platform_image(image):
    """Resolve a locally-tagged multi-platform image to the host platform's single-platform
    manifest digest and re-tag it locally.

    `kind load docker-image` fails when a locally-tagged image resolves to a multi-platform
    manifest index (e.g. anything pulled by tag under Docker's containerd-backed image store,
    which keeps the full index as the tag's descriptor even though only the host platform's
    content was downloaded). kind issues `ctr images import --all-platforms` against it, which
    then fails looking for blobs of platforms that were never pulled:
      "ctr: content digest sha256:...: not found"
    """
    proc = subprocess.run(['docker', 'image', 'inspect', image, '--format', '{{json .Descriptor.mediaType}}'],
                          capture_output=True, text=True)
    media_type = proc.stdout.strip().strip('"') if proc.returncode == 0 else ''
    if 'manifest.list' not in media_type and 'image.index' not in media_type:
        return  # already a single-platform manifest; nothing to do

    machine = platform.machine()
    if machine == 'x86_64':
        goarch = 'amd64'
    elif machine in ('aarch64', 'arm64'):
        goarch = 'arm64'
    else:
        goarch = machine
    goos = platform.system().lower()

    # `docker manifest inspect` only succeeds for images backed by a registry; locally-built
    # images (e.g. IMAGE_NAME from createImage.sh) can also report an index descriptor (build
    # attestations) but have all their content already present locally, so kind load works
    # fine as-is. A lookup failure here just leaves the image as-is rather than aborting.
    digest = None
    manifest_proc = subprocess.run(['docker', 'manifest', 'inspect', image], capture_output=True, text=True)
    if manifest_proc.returncode == 0:
        try:
            manifest = json.loads(manifest_proc.stdout)
            for entry in manifest.get('manifests', []):
                plat = entry.get('platform', {})
                if plat.get('os') == goos and plat.get('architecture') == goarch and not plat.get('variant'):
                    digest = entry.get('digest')
                    break
        except json.JSONDecodeError:
            digest = None

    if not digest:
        print(f"Could not resolve a {goos}/{goarch} manifest for {image} via registry lookup "
              "(may be a locally-built image); leaving as-is.")
        return

    repo = image.split(':', 1)[0].split('@', 1)[0]

    if subprocess.run(['docker', 'pull', f"{repo}@{digest}"]).returncode != 0:
        err(f"Could not pull {repo}@{digest}; please try running")
        err(f"  docker pull {repo}@{digest}")
        err("then run this script again")
        sys.exit(1)

    if subprocess.run(['docker', 'tag', f"{repo}@{digest}", image]).returncode != 0:
        err("Error running")
        err(f"  docker tag {repo}@{digest} {image}")
        err("Please try it manually, then run this script again")
        sys.exit(1)

def init_cluster():
    image_name = os.environ.get('IMAGE_NAME')
    init_container_image = os.environ.get('INIT_CONTAINER_IMAGE')
    armada_queue = os.environ.get('ARMADA_QUEUE')

    img_regex = re.compile(r'^[a-zA-Z0-9_./-]+:[a-zA-Z0-9_-]+$')

    if not image_name or not img_regex.match(image_name):
        err(f"IMAGE_NAME is not defined or invalid. Please set it in {SCRIPTS_DIR}/config.sh")
        sys.exit(1)

    if not init_container_image or not img_regex.match(init_container_image):
        err(f"INIT_CONTAINER_IMAGE is not defined or invalid. Please set it in {SCRIPTS_DIR}/config.sh")
        sys.exit(1)

    if not armada_queue:
        err(f"ARMADA_QUEUE is not defined. Please set it in {SCRIPTS_DIR}/config.sh")
        sys.exit(1)

    print("Checking for armadactl ..")
    if not (os.path.isfile(ARMADACTL_PATH) and os.access(ARMADACTL_PATH, os.X_OK)):
        fetch_armadactl()

    print(f"Checking if image {image_name} is available")
    if not check_image(image_name):
        err(f"Image {image_name} not found in local Docker instance.")
        err("Rebuild the image and re-run this script.")
        sys.exit(1)

    print(f"Checking if image {init_container_image} is available")
    if not check_image(init_container_image):
        print(f"Image {init_container_image} not found locally; pulling from Docker Hub.")
        
        sys_os = platform.system()
        machine = platform.machine()
        
        if sys_os == 'Darwin' and machine in ('aarch64', 'arm64'):
            arm64_busybox = 'busybox@sha256:c4e5b27bf840ba1ebd5568b6b914f6926f3559b2ad4f505b1f37aae483b907d6'
            if subprocess.run(['docker', 'pull', arm64_busybox]).returncode != 0:
                err(f"Could not pull {arm64_busybox}")
                sys.exit(1)
            if subprocess.run(['docker', 'tag', arm64_busybox, init_container_image]).returncode != 0:
                err(f"Error tagging {arm64_busybox} as {init_container_image}")
                sys.exit(1)
        else:
            if subprocess.run(['docker', 'pull', init_container_image]).returncode != 0:
                err(f"Could not pull {init_container_image}")
                sys.exit(1)

    print("Checking to see if Armada cluster is available ...")
    queue_check = subprocess.run([ARMADACTL_PATH, 'get', 'queues'], capture_output=True, text=True)
    if queue_check.returncode != 0:
        if 'connection refused' in queue_check.stderr or 'connection refused' in queue_check.stdout:
            start_armada()
            time.sleep(10)
            if not armadactl_retry('get', 'queues'):
                sys.exit(1)
        else:
            err("FAILED: output is ")
            print(queue_check.stdout)
            print(queue_check.stderr)
            err("Armada cluster appears to be running, but an unknown error has happened; exiting now")
            sys.exit(1)
    else:
        log("Armada is available")

    log("Armada Cluster Nodes")
    subprocess.run(['kubectl', 'get', 'nodes'])

    tmp_dir = os.path.join(SCRIPTS_DIR, '.tmp')
    os.makedirs(tmp_dir, exist_ok=True)

    armada_master = os.environ.get('ARMADA_MASTER', '')
    if '//localhost' in armada_master or '//host.docker.internal' in armada_master:
        kind_tool = "kind"
        env_with_tmp = os.environ.copy()
        env_with_tmp['TMPDIR'] = tmp_dir
        
        for img in [image_name, init_container_image]:
            pin_single_platform_image(img)
            log_group(f"Loading Docker image {img} into Armada (Kind) cluster")
            subprocess.run([kind_tool, 'load', 'docker-image', img, '--name', 'armada'],
                           env=env_with_tmp, check=True)
            end_log_group()

    # Configure defaults
    src_conf = os.path.join(PROJECT_ROOT, 'e2e', 'spark-defaults.conf')
    dest_conf = os.path.join(PROJECT_ROOT, 'conf', 'spark-defaults.conf')
    os.makedirs(os.path.dirname(dest_conf), exist_ok=True)
    shutil.copy(src_conf, dest_conf)

    if '//localhost' in armada_master or '//host.docker.internal' in armada_master:
        log("Waiting 60 seconds for Armada to stabilize ...")
        time.sleep(60)

def run_test():
    print("Running Scala E2E test suite...")

    os.chdir(PROJECT_ROOT)

    tls_args = []
    if os.environ.get("CLIENT_CERT_FILE"):
        tls_args.append(f"-Dclient_cert_file={os.environ['CLIENT_CERT_FILE']}")
    if os.environ.get("CLIENT_KEY_FILE"):
        tls_args.append(f"-Dclient_key_file={os.environ['CLIENT_KEY_FILE']}")
    if os.environ.get("CLUSTER_CA_FILE"):
        tls_args.append(f"-Dcluster_ca_file={os.environ['CLUSTER_CA_FILE']}")

    mvn_cmd = [
        'mvn', os.environ.get('MVN_OFFLINE', '').strip(), '-e', 'scalatest:test',
        '-Dsuites=org.apache.spark.deploy.armada.e2e.ArmadaSparkE2E',
        f"-Dcontainer.image={os.environ.get('IMAGE_NAME', '')}",
        f"-Dscala.version={os.environ.get('SCALA_VERSION', '')}",
        f"-Dscala.binary.version={os.environ.get('SCALA_BIN_VERSION', '')}",
        f"-Dspark.version={os.environ.get('SPARK_VERSION', '')}",
        f"-Darmada.queue={os.environ.get('ARMADA_QUEUE', '')}",
        f"-Darmada.master={os.environ.get('ARMADA_MASTER', '')}",
        f"-Darmada.lookout.url={os.environ.get('ARMADA_LOOKOUT_URL', '')}",
        f"-Darmadactl.path={ARMADACTL_PATH}"
    ]
    # Filter out empty arguments generated by missing env vars
    mvn_cmd = [arg for arg in mvn_cmd if arg] + tls_args

    env_with_trust = os.environ.copy()
    env_with_trust['KUBERNETES_TRUST_CERTIFICATES'] = 'true'

    try:
        with open("e2e-test.log", "w") as log_file:
            # Use Popen to tee the output to both terminal and file
            process = subprocess.Popen(mvn_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                                       text=True, env=env_with_trust)
            for line in process.stdout:
                sys.stdout.write(line)
                log_file.write(line)
            
            process.wait()
            test_exit_code = process.returncode
            
            if test_exit_code != 0:
                err(f"E2E tests failed with exit code {test_exit_code}")
                sys.exit(test_exit_code)
                
            log("E2E tests completed successfully")
            
    except Exception as e:
        err(f"Failed to execute Maven tests: {e}")
        sys.exit(1)

def main():
    init_script = os.path.join(SCRIPTS_DIR, 'init.sh')
    source_env_script(init_script)
    os.environ['PATH'] = f"{SCRIPTS_DIR}:{os.path.join(AOHOME, 'bin', 'tooling')}:{os.environ.get('PATH', '')}"
    init_cluster()
    run_test()

if __name__ == '__main__':
    main()
