#!/usr/bin/env bash

oozie job -config `dirname $0`/oozie_coordinator.properties -oozie http://ip-10-0-45-88.ap-south-1.compute.internal:11000/oozie -run