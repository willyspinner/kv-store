#!/usr/bin/env bash

###         VARIABLES TO SET          ###

if [ "${N_CORES}" == "" ]; then
    export N_CORES=8
fi
if [ "${N_TRIALS}" == "" ]; then 
    export N_TRIALS=500
fi

if [ "${N_TESTCASES}" == "" ]; then 
    export N_TESTCASES=19
fi

if [ "${TESTFUNC}" != "" ]; then 
    echo "Using testfunc ${TESTFUNC}.."
else 
    export TESTFUNC=""
fi
#export N_TESTCASES=1
#export TESTFUNC="TestManyUnreliable"
###              SCRIPT               ###

if [ "${GOPATH}" == "" ]; then 
    echo "please set GOPATH properly"
    exit 1
fi

cat << 'EOF' > testOne.sh
#!/bin/bash
LOGFILE="testfile-$1-temp"
if [ "${TESTFUNC}" == "" ]; then 
    go test &> "${LOGFILE}"
else
    go test -run "${TESTFUNC}" &> "${LOGFILE}"
fi

LOGFILE="testfile-${1}-temp"
N_PASSED="$(cat "${LOGFILE}" | grep 'Passed' | wc -l)"
if [ "${N_PASSED}" -eq "${N_TESTCASES}" ]; then
    echo "Trial ${1} PASS: ${N_PASSED} / ${N_TESTCASES} passed."
    rm "${LOGFILE}"
else
    echo "Trial ${1} FAIL: ${N_PASSED} / ${N_TESTCASES} See logfile: ${LOGFILE}"
fi
EOF

chmod u+x testOne.sh

echo "Doing ${N_TRIALS} tests on ${N_CORES} cores..."
seq "${N_TRIALS}" | xargs -P ${N_CORES} -I % ./testOne.sh %
echo "DONE"


