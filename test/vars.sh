#!/bin/bash

## ===== General environment variables for the Percona Operator tests =====
export OPERATOR_ROOT_PATH=${OPERATOR_ROOT_PATH:-${PWD}}
echo "OPERATOR_ROOT_PATH=${OPERATOR_ROOT_PATH}"

## ======= Upstream DB operators params for testing ===============

# Recommended PostgreSQL operator version for tests.
export PG_OPERATOR_VERSION=${PG_OPERATOR_VERSION:-"3.0.0"}
echo "PG_OPERATOR_VERSION=${PG_OPERATOR_VERSION}"

# Recommended PostgreSQL engine version for tests.
export PG_DB_ENGINE_VERSION=${PG_DB_ENGINE_VERSION:-"18"}
echo "PG_DB_ENGINE_VERSION=${PG_DB_ENGINE_VERSION}"

# Previous versions for upgrade tests.
export PREVIOUS_PG_DB_ENGINE_VERSION=${PREVIOUS_PG_DB_ENGINE_VERSION:-"17"}
echo "PREVIOUS_PG_DB_ENGINE_VERSION=${PREVIOUS_PG_DB_ENGINE_VERSION}"

export PREVIOUS_PG_OPERATOR_VERSION=${PREVIOUS_PG_OPERATOR_VERSION:-"2.9.0"}
echo "PREVIOUS_PG_OPERATOR_VERSION=${PREVIOUS_PG_OPERATOR_VERSION}"

## ============== K3D cluster configuration ===================
# export KUBECONFIG="${KUBECONFIG:-${OPERATOR_ROOT_PATH}/test/kubeconfig}"
# echo "KUBECONFIG=${KUBECONFIG}"
