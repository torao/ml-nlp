#!/usr/bin/env bash

docker run -it --rm -v "$PWD:/opt/data" python:latest python $*
