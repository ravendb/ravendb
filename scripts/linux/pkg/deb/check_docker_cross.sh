#!/bin/bash

docker run --platform linux/amd64 -it --rm ubuntu uname -m
docker run --platform linux/arm64 -it --rm ubuntu uname -m
docker run --platform linux/arm/v7 -it --rm ubuntu uname -m
