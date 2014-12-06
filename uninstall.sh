#!/bin/bash

SOURCE="/usr/bin/impala-shell"
while [ -h "$SOURCE" ]
do
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
  BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
done
BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
LIB_DIR=$BIN_DIR/../lib

sudo rm $BIN_DIR/blinkdb-shell
sudo rm $LIB_DIR/impala-shell/blinkdb_shell.py
sudo rm /usr/bin/blinkdb-shell
