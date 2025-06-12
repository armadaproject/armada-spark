#!/bin/bash

if [ "${RUNNING_E2E_TESTS}" == "true" ]; then
    #Generate e2e data for extraFiles directory
    rm -rf $scripts/../extraFiles/*
    echo Copying extrafiles from $image_prefix:$image_tag
    id=$(docker create $image_prefix:$image_tag)
    docker cp $id:/opt/spark/examples/jars  $scripts/../extraFiles
    docker cp $id:/opt/spark/examples/src  $scripts/../extraFiles
    docker rm -v $id
fi
