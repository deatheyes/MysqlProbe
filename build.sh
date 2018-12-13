#!/bin/sh
# build
DATE=$(date +%FT%T%z)
BRANCH=$(git symbolic-ref --short -q HEAD)
SHA1=$(git rev-parse HEAD)
go build -ldflags "-X main.branch=${BRANCH} -X main.date=${DATE} -X main.sha1=${SHA1}"
