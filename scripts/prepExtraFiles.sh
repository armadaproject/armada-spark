#!/bin/bash

if [ "${RUNNING_E2E_TESTS}" == "true" ]; then
    #Generate e2e data for extraFiles directory
    rm -rf $scripts/../extraFiles/*
    # Base images are tagged per arch as `<prefix>:<tag>-<arch>` by
    # createImage.sh; the extracted example jars are platform agnostic, so the
    # host-arch copy serves.
    base_image="$image_prefix:$image_tag-$HOST_ARCH"
    echo Copying extrafiles from $base_image
    id=$(docker create $base_image)
    docker cp $id:/opt/spark/examples/jars  $scripts/../extraFiles
    docker cp $id:/opt/spark/examples/src  $scripts/../extraFiles
    docker rm -v $id
fi
