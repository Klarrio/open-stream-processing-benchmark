#!/usr/bin/env bash
docker build -t `cat version` .
docker push `cat version`
