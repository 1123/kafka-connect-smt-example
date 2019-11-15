#!/bin/bash

set -u -e

CONNECTOR_NAME=$1

curl localhost:8083/connectors/$CONNECTOR_NAME | jq .
