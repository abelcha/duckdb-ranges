#pragma once

#include "duckdb.hpp"

namespace duckdb {

class RangesExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

LogicalType GetInt4RangeType();

} // namespace duckdb
