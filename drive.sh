#!/bin/bash
#sed line is for convenient running of the node step in a quite constrained docker

targdir="$(dirname $(readlink -f ${BASH_SOURCE}))"
echo $targdir
rm -f "${targdir}"/for_bucket_1/*LOG && \
cd "${targdir}"/for_bucket_1/ && \
rm -rf snapshots/ && \
mkdir snapshots/ && \
cp -r ../../las_utils/rehydration/snapshots/*/ snapshots/ && \
../changes.py snapshots/*/ 2> CHANGE_ERR_LOG > CHANGE_LOG && \
sed "s@^\([[:digit:]]\+:\(service\|name\):[[:digit:]_-]\+:\)${targdir}/for_bucket_1/\(.*\)@\1\3@" CHANGE_LOG > rel_CHANGE_LOG && \
cat CHANGE_LOG | node ../convert.js 2> CONVERT_ERR_LOG | tee LOG
