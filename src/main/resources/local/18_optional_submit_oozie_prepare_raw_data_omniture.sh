#!/usr/bin/env bash

oozie job -config `dirname $0`/prepare_input_data_oozie.properties -oozie http://quickstart.cloudera:11000/oozie -run