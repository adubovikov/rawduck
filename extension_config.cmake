# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(rawduck
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# JSON extension: provides the JSON logical type and to_json()/json_* functions
# that RawDuck relies on for its structural-conflict columns
duckdb_extension_load(json)
