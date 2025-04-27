#!/bin/bash

cd /covid19_dbt
dbt deps
dbt seed
dbt compile
