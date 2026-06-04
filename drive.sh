#!/bin/bash

outdir="$(mktemp -d /tmp/for_bucket.XXXXX)" && \
scriptdir="$(dirname $(readlink -f ${BASH_SOURCE}))" && \
mkdir -p "${scriptdir}/for_bucket/" && \
mkdir -p "${scriptdir}/cache/" && \
cpdir="n_$(ls ${scriptdir}/for_bucket | wc -l)" && \
cd "${outdir}" && \
mkdir snapshots/ && \
cp -r "${scriptdir}"/../las_utils/rehydration/snapshots/*/ snapshots/ && \
${scriptdir}/changes.py snapshots/*/ 2> CHANGE_ERR_LOG > CHANGE_LOG && \
sed "s@^\([[:digit:]]\+:\(service\|name\):[[:digit:]_-]\+:\)${outdir}/\(.*\)@\1\3@" CHANGE_LOG > rel_CHANGE_LOG && \
cat <<EOF >${outdir}/d_drive.sh
#!/bin/sh
cd ..
cp -r las_fe a
cd a
npm i
cd for_bucket
mkdir output
cat rel_CHANGE_LOG | node ../convert.js 2> CONVERT_ERR_LOG | tee CONVERT_LOG
echo 'Now exit docker by pressing enter here, after running (on host):'
read -p "docker cp \${HOSTNAME}:/app/a/for_bucket ${scriptdir}/for_bucket/${cpdir}"
EOF
if [ x"$?" != 'x0' ]; then
  exit 1
fi
chmod 755 "${outdir}/d_drive.sh" && \
docker run --mount type=bind,src="${scriptdir}",dst=/app/las_fe,ro \
           --mount type=bind,src="${outdir}",dst=/app/las_fe/for_bucket,ro \
           --mount type=bind,src="${scriptdir}/../las_utils/BACKEND_CACHE_WITH_LINKS",dst=/app/las_fe/cache,ro \
           -it \
           --entrypoint '/app/las_fe/for_bucket/d_drive.sh' \
           --rm las
rmdir "${scriptdir}/cache" #just to avoid confusing myself -- rmdir is very safe, unlike rm -rf
