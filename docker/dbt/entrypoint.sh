#!/bin/bash

cd /transformer
dbt deps
dbt seed
dbt compile
