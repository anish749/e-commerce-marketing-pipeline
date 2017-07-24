#!/usr/bin/env bash

oozie job -config `dirname $0`/setup_hive_oozie.properties -oozie http://quickstart.cloudera:11000/oozie -run