#!/bin/bash

cd /transformer
dbt deps
dbt seed
dbt compile --vars '{"min_date": "2020-01-22", "max_date": "2020-01-23"}'
