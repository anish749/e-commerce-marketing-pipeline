#!/usr/bin/env bash

oozie job -config `dirname $0`/setup_hive_oozie.properties -oozie http://ip-10-0-45-88.ap-south-1.compute.internal:11000/oozie -run