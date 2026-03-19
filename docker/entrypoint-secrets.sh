#!/bin/bash
# Read Docker secrets into environment variables before exec-ing Airflow.
# Docker secrets are mounted as files under /run/secrets/.
# This script bridges the gap for services that don't natively support _FILE suffix.

if [ -f /run/secrets/fernet_key ]; then
  export AIRFLOW__CORE__FERNET_KEY=$(cat /run/secrets/fernet_key)
fi

if [ -f /run/secrets/redis_password ]; then
  export REDIS_PASSWORD=$(cat /run/secrets/redis_password)
fi

exec /entrypoint "$@"
