#!/bin/bash

set -e -u

CONNECTOR_NAME=$1

curl -X DELETE localhost:8083/connectors/$CONNECTOR_NAME
