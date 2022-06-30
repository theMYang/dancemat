#!/bin/bash

set -e
set -x

ROOT=$(cd $(dirname ${BASH_SOURCE:-$0});pwd)
java -cp $ROOT/../target/dancemat-1.0-SNAPSHOT-jar-with-dependencies.jar com.bytedance.dancemat.tools.ReadFooter $1
