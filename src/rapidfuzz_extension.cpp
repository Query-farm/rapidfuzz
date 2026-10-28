#define DUCKDB_EXTENSION_MAIN

#include "rapidfuzz_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "query_farm_telemetry.hpp"
#include "rapidfuzz_scorer.hpp"

namespace duckdb {

static constexpr const char *RAPIDFUZZ_VERSION = "2026102801";

void RegisterDocumentedFunction(ExtensionLoader &loader, ScalarFunction function, FunctionDocumentation documentation) {
	CreateScalarFunctionInfo info(std::move(function));
	FunctionDescription description;
	description.description = std::move(documentation.description);
	description.examples = std::move(documentation.examples);
	description.categories = std::move(documentation.categories);
	info.descriptions.push_back(std::move(description));
	loader.RegisterFunction(std::move(info));
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Fuzzy string matching and string similarity metrics powered by RapidFuzz");
	RegisterFuzzFunctions(loader);
	RegisterDistanceFunctions(loader);
	RegisterAlignmentFunctions(loader);
	RegisterExtractFunctions(loader);
	QueryFarmSendTelemetry(loader, "rapidfuzz", RAPIDFUZZ_VERSION);
}

void RapidfuzzExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string RapidfuzzExtension::Name() {
	return "rapidfuzz";
}

std::string RapidfuzzExtension::Version() const {
	return RAPIDFUZZ_VERSION;
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rapidfuzz, loader) {
	duckdb::LoadInternal(loader);
}
}
