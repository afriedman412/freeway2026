#!/usr/bin/env bash

docker run --rm -d \
  --name test_db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=test_db \
  -p 5433:5432 \
  postgres:16
