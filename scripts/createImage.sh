#!/bin/bash
set -e

echo Generating armada spark docker image.

root="$(cd "$(dirname "$0")/.."; pwd)"
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

# Default builds for host arch only (fast, native). PUSH=true builds both
# linux/amd64 and linux/arm64 and pushes a multi-arch manifest list to Docker
# Hub under $IMAGE_NAME. Cross-arch base build runs under QEMU emulation and
# is slow the first time, but per-arch base images are cached afterward.
HOST_ARCH=$(uname -m | sed 's|x86_64|amd64|;s|aarch64|arm64|')
if [ "${PUSH:-false}" = "true" ]; then
    TARGET_PLATFORMS=(linux/amd64 linux/arm64)
else
    TARGET_PLATFORMS=("linux/$HOST_ARCH")
fi

image_prefix=apache/spark

if [ "$USE_DISTRIBUTED_SHUFFLE_STORAGE" == "true" ]; then
    image_prefix="${DSS_PREFIX:-gbj262/spark.dss.img2}"
    image_tag="${DSS_TAG:-latest}"
# There are no Docker images for Spark 3 and Scala 2.13, as well as for Spark 3.3.4 and any Scala
elif [[ "$SPARK_VERSION" == "3."* ]] && ( [[ "$SCALA_BIN_VERSION" == "2.13" ]] || [[ "$SPARK_VERSION" == "3.3.4" ]] ); then
        echo Checking for images for spark: $SPARK_VERSION scala: $SCALA_BIN_VERSION
        if [ "${INCLUDE_PYTHON}" == "false" ]; then
            echo Building image without Python.
            image_prefix=spark
            extra_build_params=""
        else
            echo Building Python image.
            image_prefix=spark-py
            extra_build_params=" -p ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile "
        fi
        # Build base image per target platform, tagging with -$arch suffix.
        # The app Dockerfile picks the right one via $TARGETARCH in FROM.
        missing_platforms=()
        for plat in "${TARGET_PLATFORMS[@]}"; do
            arch="${plat##*/}"
            if ! docker image inspect "$image_prefix:$image_tag-$arch" >/dev/null 2>&1; then
                missing_platforms+=("$plat")
            fi
        done
        if [ "${#missing_platforms[@]}" -gt 0 ]; then
            echo "There are no Docker images released for Spark $SPARK_VERSION and Scala $SCALA_BIN_VERSION."
            if [[ ! -d ".spark-$SPARK_VERSION" ]]; then
                echo "Checking out Spark sources for tag v$SPARK_VERSION."
                git clone https://github.com/apache/spark --branch v$SPARK_VERSION --depth 1 --no-tags ".spark-$SPARK_VERSION"
            fi
            cd ".spark-$SPARK_VERSION"
            # Spark 3.3.4 does not compile without this fix
            if [[ "$SPARK_VERSION" == "3.3.4" ]]; then
                sed -i -e "s%<scala.version>2.13.8</scala.version>%<scala.version>2.13.6</scala.version>%" pom.xml
                # Fix deprecated openjdk base image - use eclipse-temurin:11-jammy instead.
                spark_dockerfile="resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile"
                if [ -f "$spark_dockerfile" ]; then
                    sed -i -e 's|FROM openjdk:|FROM eclipse-temurin:|g' "$spark_dockerfile"
                    sed -i -E 's/^ARG java_image_tag=11-jre-slim$/ARG java_image_tag=11-jammy/' "$spark_dockerfile"
                fi
            fi
            ./dev/change-scala-version.sh $SCALA_BIN_VERSION
            # by packaging the assembly project specifically, jars of all depending Spark projects are fetch from Maven
            # spark-examples jars are not released, so we need to build these from sources.
            # The jars are platform agnostic, so a single Maven build serves every
            # target platform; only the docker image build below runs per arch.
            ./build/mvn ${MVN_OFFLINE-} --batch-mode clean
            ./build/mvn ${MVN_OFFLINE-} --batch-mode package -pl examples
            ./build/mvn ${MVN_OFFLINE-} --batch-mode package -Pkubernetes -Phadoop-cloud -Pscala-$SCALA_BIN_VERSION -pl assembly
            for plat in "${missing_platforms[@]}"; do
                arch="${plat##*/}"
                echo "Building Spark Docker image $image_prefix:$image_tag-$arch for $plat from source."
                DOCKER_DEFAULT_PLATFORM="$plat" ./bin/docker-image-tool.sh -t "$image_tag" $extra_build_params build
                # docker-image-tool.sh tags as `$image_prefix:$image_tag`; rename to the
                # arch-suffixed tag so each platform keeps a distinct base image.
                docker tag "$image_prefix:$image_tag" "$image_prefix:$image_tag-$arch"
                docker rmi "$image_prefix:$image_tag" 2>/dev/null || true
            done
            cd ..
        fi
fi


# The app Dockerfile pins its base as `<prefix>:<tag>-<arch>`. Bases built
# from source above are tagged that way already; registry bases (official
# apache/spark or DSS images) publish multi-arch manifests under the bare
# tag, so pull each platform and retag it with the arch suffix.
for plat in "${TARGET_PLATFORMS[@]}"; do
    arch="${plat##*/}"
    if ! docker image inspect "$image_prefix:$image_tag-$arch" >/dev/null 2>&1; then
        echo "Pulling base $image_prefix:$image_tag for $plat..."
        docker pull --platform "$plat" "$image_prefix:$image_tag"
        docker tag "$image_prefix:$image_tag" "$image_prefix:$image_tag-$arch"
        docker rmi "$image_prefix:$image_tag" >/dev/null 2>&1 || true
    fi
done

source "$scripts/prepExtraFiles.sh"

build_args=(
  --build-arg spark_base_image_prefix=$image_prefix
  --build-arg spark_base_image_tag=$image_tag
  --build-arg scala_binary_version=$SCALA_BIN_VERSION
  --build-arg spark_version=$SPARK_VERSION
  --build-arg include_python=$INCLUDE_PYTHON
  -f "$root/docker/Dockerfile"
)

# Each platform is built separately with plain `docker build`, which always
# resolves FROM against the daemon's local image store. This keeps the
# locally-tagged base images visible regardless of buildx builder type or
# image store (classic or containerd); a single multi-platform buildx build
# would require a builder that cannot see local images. 
# BuildKit is needed for TARGETARCH and --platform.
export DOCKER_BUILDKIT=1

if [ "${PUSH:-false}" = "true" ]; then
    # Build and push one image per arch, then assemble them into a multi-arch
    # manifest list under the bare $IMAGE_NAME. imagetools talks to the
    # registry directly, so this works with any builder configuration.
    ARCH_TAGS=()
    for plat in "${TARGET_PLATFORMS[@]}"; do
        arch="${plat##*/}"
        case "$IMAGE_NAME" in
            *:*) arch_tag="$IMAGE_NAME-$arch" ;;
            *)   arch_tag="$IMAGE_NAME:$arch" ;;
        esac
        echo "Building $arch_tag from base $image_prefix:$image_tag-$arch..."
        docker build --platform "$plat" --tag "$arch_tag" "${build_args[@]}" "$root"
        docker push "$arch_tag"
        ARCH_TAGS+=("$arch_tag")
    done
    docker buildx imagetools create --tag "$IMAGE_NAME" "${ARCH_TAGS[@]}"
else
    echo "using spark image: $image_prefix:$image_tag-$HOST_ARCH"
    docker build --platform "linux/$HOST_ARCH" --tag "$IMAGE_NAME" "${build_args[@]}" "$root"
fi
