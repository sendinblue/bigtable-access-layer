#!/usr/bin/env bash

export BIGTABLE_EMULATOR_HOST=127.0.0.1:8086
num_tables=`cbt -project example-project -instance example-instance ls | wc -l`

if [ $num_tables -ne 0 ]; then
  echo "ecommerce_events table already exists, exiting"
  exit 0
fi
cbt -project example-project -instance example-instance createtable ecommerce_events

cbt -project example-project -instance example-instance createfamily ecommerce_events front

echo "table created successfully"

exit 0
