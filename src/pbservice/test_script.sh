#!/usr/bin/env bash

###         VARIABLES TO SET          ###

N_TESTS=50
N_CORES=8

###              SCRIPT               ###

if [ "${GOPATH}" == "" ]; then 
    echo "please set GOPATH properly"
    exit 1
fi

if [ ! -e testOne.sh ]; then
    cat << 'EOF' > testOne.sh
#!/bin/bash
LOGFILE="testfile-$1-temp"
go test &> "${LOGFILE}"
LOGFILE="testfile-${1}-temp"
N_PASSED="$(cat "${LOGFILE}" | grep 'Passed' | wc -l)"
if [ "${N_PASSED}" -eq "15" ]; then
    echo "Trial PASS: ${N_PASSED} / 15 passed in logfile ${LOGFILE}"
    rm "${LOGFILE}"
else
    echo "Trial FAIL: ${N_PASSED} / 15 passed in logfile ${LOGFILE}"
fi
EOF
fi

chmod u+x testOne.sh

echo "Doing ${N_TESTS} tests on ${N_CORES} cores..."
seq "${N_TESTS}" | xargs -P ${N_CORES} -I % ./testOne.sh %


