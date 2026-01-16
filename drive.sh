#!/bin/bash

targdir="$(dirname $(readlink -f ${BASH_SOURCE}))"
rm "${targdir}"/for_bucket/* && \
cd "${targdir}"/for_bucket/ && \
../changes.py ~/c_home/Downloads/SNAPSHOTS/for_sp/202* 2> CHANGE_ERR_LOG | node ../convert.js 2> CONVERT_ERR_LOG | tee LOG
