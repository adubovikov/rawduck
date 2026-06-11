PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=rawduck
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# The OTLP/gRPC server is opt-in (it pulls the gRPC/protobuf stack and
# significantly lengthens builds): make release RAWDUCK_ENABLE_GRPC=1
ifeq ($(RAWDUCK_ENABLE_GRPC),1)
	EXT_FLAGS:=$(EXT_FLAGS) -DRAWDUCK_ENABLE_GRPC=1 -DVCPKG_MANIFEST_FEATURES=grpc
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile