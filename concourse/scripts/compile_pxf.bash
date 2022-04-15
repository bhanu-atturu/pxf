#!/bin/bash

set -eoux pipefail

GPHOME=/usr/local/greenplum-db-devel
PXF_ARTIFACTS_DIR=${PWD}/${OUTPUT_ARTIFACT_DIR}

source "${GPHOME}/greenplum_path.sh"

# use a login shell for setting environment
bash --login -c "
	source ~/.pxfrc
	make -C '${PWD}/pxf_src' test install
"

# Create tarball for PXF
tar -C "${GPHOME}" -czf "${PXF_ARTIFACTS_DIR}/pxf.tar.gz" pxf
