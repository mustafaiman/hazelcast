#!/bin/bash

CLASS_NAME=$1

PACKAGES=$(echo $CLASS_NAME | tr "." "\n")

for PACK in $PACKAGES
do
    PATH_C="${PATH_C}${PACK}_"
done
PATH_C="${PATH_C%?}"
echo $PATH_C
export JAVA_HOME="$(/usr/libexec/java_home)"
SOURCE_PATH="hazelcast/src/main/resources"
echo "JAVA_HOME is set to $JAVA_HOME"
gcc -I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin -O3 -o $SOURCE_PATH/libSIMDHelper.jnilib -shared $SOURCE_PATH/${PATH_C}.c
