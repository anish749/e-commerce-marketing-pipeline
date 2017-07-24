#!/usr/bin/env bash

oozie job -config `dirname $0`/oozie_coordinator.properties -oozie http://quickstart.cloudera:11000/oozie -run