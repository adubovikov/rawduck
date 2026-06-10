#include "raw_functions.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ATTACH 'rawduck:store.db' AS raw
//
// A RawDuck store is a native DuckDB database under a RawDuck-typed catalog:
// every regular query (SELECT, INSERT, CTAS, UPDATE, ...) runs through
// DuckDB's own storage at full native speed, while the catalog type marks the
// database as a RawMergeTree store for RawDuck's schema-less ingestion and
// adaptive-layout machinery.
//===--------------------------------------------------------------------===//

class RawDuckCatalog : public DuckCatalog {
public:
	explicit RawDuckCatalog(AttachedDatabase &db) : DuckCatalog(db) {
	}

	string GetCatalogType() override {
		return "rawduck";
	}
};

static unique_ptr<Catalog> RawDuckAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                         AttachedDatabase &db, const string &name, AttachInfo &info,
                                         AttachOptions &options) {
	return make_uniq_base<Catalog, RawDuckCatalog>(db);
}

static unique_ptr<TransactionManager> RawDuckCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                      AttachedDatabase &db, Catalog &catalog) {
	return make_uniq_base<TransactionManager, DuckTransactionManager>(db);
}

shared_ptr<StorageExtension> GetRawDuckStorageExtension() {
	auto extension = make_shared_ptr<StorageExtension>();
	extension->attach = RawDuckAttach;
	extension->create_transaction_manager = RawDuckCreateTransactionManager;
	return extension;
}

} // namespace duckdb
