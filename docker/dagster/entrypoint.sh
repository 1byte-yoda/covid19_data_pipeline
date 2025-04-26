#!/bin/bash

cd /covid19_dbt
dbt deps
dbt seed
dbt compile

cd /pipeline
exec dagster dev -h 0.0.0.0
