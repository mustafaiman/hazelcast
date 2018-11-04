#!/bin/bash

CLASS_NAME=$1

PACKAGES=$(echo $CLASS_NAME | tr "." "\n")

PATH_C=$(pwd)
PATH_C="${PATH_C}/hazelcast/src/main/java"
for PACK in $PACKAGES
do
    PATH_C="${PATH_C}/${PACK}"
done

echo $PATH_C
javac -h "hazelcast/src/main/resources/." "${PATH_C}.java"
