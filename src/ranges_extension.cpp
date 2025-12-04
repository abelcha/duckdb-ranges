#include <cstdint>
#include <string>
#define DUCKDB_EXTENSION_MAIN

#include "ranges_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

struct Int4Range {
	int32_t lower;
	int32_t upper;
	bool lower_inc;
	bool upper_inc;
};

LogicalType GetInt4RangeType() {
	auto type = LogicalType(LogicalTypeId::BLOB);
	type.SetAlias("INT4RANGE");
	return type;
}

static string_t SerializeInt4Range(const Int4Range &range, Vector &result) {
	const size_t size = sizeof(int32_t) * 2 + sizeof(uint8_t);
	auto data = StringVector::EmptyString(result, size);
	auto ptr = data.GetDataWriteable();
	memcpy(ptr, &range.lower, sizeof(int32_t));
	memcpy(ptr + sizeof(int32_t), &range.upper, sizeof(int32_t));
	// Pack the two booleans into a single byte: bit 1 = lower_inc, bit 0 = upper_inc
	uint8_t bounds = (range.lower_inc ? 0b10 : 0) | (range.upper_inc ? 0b01 : 0);
	memcpy(ptr + sizeof(int32_t) * 2, &bounds, sizeof(uint8_t));
	return data;
}

static Int4Range DeserializeInt4Range(const string_t &blob) {
	const size_t expected_size = sizeof(int32_t) * 2 + sizeof(uint8_t);
	if (blob.GetSize() < expected_size) {
		throw InvalidInputException("Invalid INT4RANGE blob: expected %zu bytes, got %zu", expected_size,
		                            blob.GetSize());
	}
	Int4Range range;
	auto ptr = blob.GetDataUnsafe();
	memcpy(&range.lower, ptr, sizeof(int32_t));
	memcpy(&range.upper, ptr + sizeof(int32_t), sizeof(int32_t));
	// Unpack the byte into two booleans
	uint8_t bounds;
	memcpy(&bounds, ptr + sizeof(int32_t) * 2, sizeof(uint8_t));
	range.lower_inc = (bounds & 0b10) != 0;
	range.upper_inc = (bounds & 0b01) != 0;
	return range;
}

static bool IsEmpty(const Int4Range &range) {
	// If lower > upper, it's empty.
	// If lower == upper, it's empty unless both bounds are inclusive [].
	if (range.lower > range.upper) {
		return true;
	}
	if (range.lower == range.upper) {
		return !(range.lower_inc && range.upper_inc); // Only [] is non-empty for equal bounds
	}
	return false;
}

static string_t ConstructInt4RangeValue(Vector &result, int32_t lower, int32_t upper, bool lower_inc, bool upper_inc) {
	Int4Range range = {lower, upper, lower_inc, upper_inc};
	return SerializeInt4Range(range, result);
}
static Int4Range ParseInt4Range(const string &input_str) {
	const string &s = input_str;

	if (StringUtil::CIEquals(s, "empty")) {
		return {1, 0, false, false}; // Canonical empty
	}

	if (s.length() < 3) {
		throw InvalidInputException("Malformed range literal: \"%s\"", input_str);
	}

	bool lower_inc;
	if (s.front() == '[') {
		lower_inc = true;
	} else if (s.front() == '(') {
		lower_inc = false;
	} else {
		throw InvalidInputException("Malformed range literal: \"%s\"", input_str);
	}

	bool upper_inc;
	if (s.back() == ']') {
		upper_inc = true;
	} else if (s.back() == ')') {
		upper_inc = false;
	} else {
		throw InvalidInputException("Malformed range literal: \"%s\"", input_str);
	}

	auto comma_pos = s.find(',');
	if (comma_pos == string::npos) {
		throw InvalidInputException("Malformed range literal: \"%s\" (missing comma)", input_str);
	}

	string lower_str = s.substr(1, comma_pos - 1);
	string upper_str = s.substr(comma_pos + 1, s.length() - comma_pos - 2);

	int32_t lower, upper;
	try {
		lower = std::stoi(lower_str);
		upper = std::stoi(upper_str);
	} catch (...) {
		throw InvalidInputException("Invalid integer in range literal: \"%s\"", input_str);
	}

	return {lower, upper, lower_inc, upper_inc};
}

static void Int4RangeConstructor4(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lower_vec = args.data[0];
	auto &upper_vec = args.data[1];
	auto &lower_inc_vec = args.data[2];
	auto &upper_inc_vec = args.data[3];
	idx_t count = args.size();

	UnifiedVectorFormat lower_data, upper_data, lower_inc_data, upper_inc_data;
	lower_vec.ToUnifiedFormat(count, lower_data);
	upper_vec.ToUnifiedFormat(count, upper_data);
	lower_inc_vec.ToUnifiedFormat(count, lower_inc_data);
	upper_inc_vec.ToUnifiedFormat(count, upper_inc_data);

	auto lower_ptr = UnifiedVectorFormat::GetData<int32_t>(lower_data);
	auto upper_ptr = UnifiedVectorFormat::GetData<int32_t>(upper_data);
	auto lower_inc_ptr = UnifiedVectorFormat::GetData<bool>(lower_inc_data);
	auto upper_inc_ptr = UnifiedVectorFormat::GetData<bool>(upper_inc_data);

	for (idx_t i = 0; i < count; i++) {
		auto lower_idx = lower_data.sel->get_index(i);
		auto upper_idx = upper_data.sel->get_index(i);
		auto lower_inc_idx = lower_inc_data.sel->get_index(i);
		auto upper_inc_idx = upper_inc_data.sel->get_index(i);

		if (!lower_data.validity.RowIsValid(lower_idx) || !upper_data.validity.RowIsValid(upper_idx) ||
		    !lower_inc_data.validity.RowIsValid(lower_inc_idx) || !upper_inc_data.validity.RowIsValid(upper_inc_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto serialized = ConstructInt4RangeValue(result, lower_ptr[lower_idx], upper_ptr[upper_idx],
		                                          lower_inc_ptr[lower_inc_idx], upper_inc_ptr[upper_inc_idx]);
		FlatVector::GetData<string_t>(result)[i] = serialized;
	}
}

static void Int4RangeConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lower_vec = args.data[0];
	auto &upper_vec = args.data[1];
	auto &bounds_vec = args.data[2];

	TernaryExecutor::Execute<int32_t, int32_t, string_t, string_t>(
	    lower_vec, upper_vec, bounds_vec, result, args.size(), [&](int32_t lower, int32_t upper, string_t bounds_blob) {
		    string bounds_str = bounds_blob.GetString();
		    bool lower_inc = true;
		    bool upper_inc = false;

		    if (!bounds_str.empty()) {
			    if (bounds_str == "[)") {
				    lower_inc = true;
				    upper_inc = false;
			    } else if (bounds_str == "[]") {
				    lower_inc = true;
				    upper_inc = true;
			    } else if (bounds_str == "(]") {
				    lower_inc = false;
				    upper_inc = true;
			    } else if (bounds_str == "()") {
				    lower_inc = false;
				    upper_inc = false;
			    } else {
				    throw InvalidInputException("Invalid bounds: " + bounds_str);
			    }
		    }
		    return ConstructInt4RangeValue(result, lower, upper, lower_inc, upper_inc);
	    });
}

static bool Int4RangeToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnifiedVectorFormat source_data;
	source.ToUnifiedFormat(count, source_data);
	auto source_ptr = UnifiedVectorFormat::GetData<string_t>(source_data);

	for (idx_t i = 0; i < count; i++) {
		auto idx = source_data.sel->get_index(i);
		if (!source_data.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto range = DeserializeInt4Range(source_ptr[idx]);
		string str;
		if (IsEmpty(range)) {
			str = "empty";
		} else {
			str = (range.lower_inc ? "[" : "(") + std::to_string(range.lower) + "," + std::to_string(range.upper) +
			      (range.upper_inc ? "]" : ")");
		}
		FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, str);
	}
	return true;
}

static bool VarcharToInt4RangeCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnifiedVectorFormat source_data;
	source.ToUnifiedFormat(count, source_data);
	auto source_ptr = UnifiedVectorFormat::GetData<string_t>(source_data);

	for (idx_t i = 0; i < count; i++) {
		auto idx = source_data.sel->get_index(i);
		if (!source_data.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		Int4Range range = ParseInt4Range(source_ptr[idx].GetString());
		auto serialized = SerializeInt4Range(range, result);
		FlatVector::GetData<string_t>(result)[i] = serialized;
	}
	return true;
}

// 1-arg constructor: int4range(varchar)
static void Int4RangeConstructor1(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vec = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(input_vec, result, args.size(), [&](string_t input) {
		Int4Range range = ParseInt4Range(input.GetString());
		return SerializeInt4Range(range, result);
	});
}

static void RangeOverlaps(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &r1_vec = args.data[0];
	auto &r2_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, bool>(
	    r1_vec, r2_vec, result, args.size(), [&](string_t r1_blob, string_t r2_blob) {
		    auto r1 = DeserializeInt4Range(r1_blob);
		    auto r2 = DeserializeInt4Range(r2_blob);

		    if (IsEmpty(r1) || IsEmpty(r2))
			    return false;

		    // Overlap logic: not (r1 entirely left of r2 or r1 entirely right of r2)
		    // r1 left of r2: r1.upper < r2.lower OR (r1.upper == r2.lower AND (!r1.upper_inc OR !r2.lower_inc))

		    bool r1_left_of_r2 = (r1.upper < r2.lower) || (r1.upper == r2.lower && (!r1.upper_inc || !r2.lower_inc));
		    bool r2_left_of_r1 = (r2.upper < r1.lower) || (r2.upper == r1.lower && (!r2.upper_inc || !r1.lower_inc));

		    return !(r1_left_of_r2 || r2_left_of_r1);
	    });
}

static void RangeContains(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &range_vec = args.data[0];
	auto &value_vec = args.data[1];

	BinaryExecutor::Execute<string_t, int32_t, bool>(
	    range_vec, value_vec, result, args.size(), [&](string_t range_blob, int32_t value) {
		    auto range = DeserializeInt4Range(range_blob);

		    if (IsEmpty(range))
			    return false;

		    // Check if value is within bounds
		    bool above_lower = (value > range.lower) || (value == range.lower && range.lower_inc);
		    bool below_upper = (value < range.upper) || (value == range.upper && range.upper_inc);

		    return above_lower && below_upper;
	    });
}

// Accessor: lower(INT4RANGE) -> INTEGER
static void RangeLower(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &range_vec = args.data[0];
	UnaryExecutor::Execute<string_t, int32_t>(range_vec, result, args.size(), [&](string_t range_blob) {
		auto range = DeserializeInt4Range(range_blob);
		return range.lower;
	});
}

// Accessor: upper(INT4RANGE) -> INTEGER
static void RangeUpper(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &range_vec = args.data[0];
	UnaryExecutor::Execute<string_t, int32_t>(range_vec, result, args.size(), [&](string_t range_blob) {
		auto range = DeserializeInt4Range(range_blob);
		return range.upper;
	});
}

// Accessor: isempty(INT4RANGE) -> BOOLEAN
static void RangeIsEmpty(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &range_vec = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(range_vec, result, args.size(), [&](string_t range_blob) {
		auto range = DeserializeInt4Range(range_blob);
		return IsEmpty(range);
	});
}

// Accessor: lower_inc(INT4RANGE) -> BOOLEAN
static void RangeLowerInc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &range_vec = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(range_vec, result, args.size(), [&](string_t range_blob) {
		auto range = DeserializeInt4Range(range_blob);
		return range.lower_inc;
	});
}

// Accessor: upper_inc(INT4RANGE) -> BOOLEAN
static void RangeUpperInc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &range_vec = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(range_vec, result, args.size(), [&](string_t range_blob) {
		auto range = DeserializeInt4Range(range_blob);
		return range.upper_inc;
	});
}

// 2-arg constructor: int4range(lower, upper) with default bounds '[)'
static void Int4RangeConstructor2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lower_vec = args.data[0];
	auto &upper_vec = args.data[1];

	BinaryExecutor::Execute<int32_t, int32_t, string_t>(
	    lower_vec, upper_vec, result, args.size(),
	    [&](int32_t lower, int32_t upper) { return ConstructInt4RangeValue(result, lower, upper, true, false); });
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterType("INT4RANGE", GetInt4RangeType());

	// Constructor: int4range(lower INT, upper INT, bounds VARCHAR) -> INT4RANGE
	ScalarFunction int4range_fun("int4range", {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::VARCHAR},
	                             GetInt4RangeType(), Int4RangeConstructor);
	loader.RegisterFunction(int4range_fun);

	// Constructor: int4range(lower INT, upper INT) -> INT4RANGE (default bounds '[)')
	ScalarFunction int4range_fun2("int4range", {LogicalType::INTEGER, LogicalType::INTEGER}, GetInt4RangeType(),
	                              Int4RangeConstructor2);
	loader.RegisterFunction(int4range_fun2);

	// Constructor: int4range(varchar) -> INT4RANGE
	ScalarFunction int4range_fun1("int4range", {LogicalType::VARCHAR}, GetInt4RangeType(), Int4RangeConstructor1);
	loader.RegisterFunction(int4range_fun1);

	// Constructor: int4range(lower INT, upper INT, lower_inc BOOLEAN, upper_inc BOOLEAN) -> INT4RANGE
	ScalarFunction int4range_fun4(
	    "int4range", {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	    GetInt4RangeType(), Int4RangeConstructor4);
	loader.RegisterFunction(int4range_fun4);

	// Cast: INT4RANGE -> VARCHAR
	loader.RegisterCastFunction(GetInt4RangeType(), LogicalType::VARCHAR, BoundCastInfo(Int4RangeToVarchar), 1);

	// Cast: VARCHAR -> INT4RANGE
	loader.RegisterCastFunction(LogicalType::VARCHAR, GetInt4RangeType(), BoundCastInfo(VarcharToInt4RangeCast), 1);

	// Operator: range_overlaps(INT4RANGE, INT4RANGE) -> BOOLEAN
	ScalarFunction overlaps_fun("range_overlaps", {GetInt4RangeType(), GetInt4RangeType()}, LogicalType::BOOLEAN,
	                            RangeOverlaps);
	loader.RegisterFunction(overlaps_fun);

	// Operator: range_contains(INT4RANGE, INTEGER) -> BOOLEAN
	ScalarFunction contains_fun("range_contains", {GetInt4RangeType(), LogicalType::INTEGER}, LogicalType::BOOLEAN,
	                            RangeContains);
	loader.RegisterFunction(contains_fun);

	// Accessor: lower(INT4RANGE) -> INTEGER
	ScalarFunction lower_fun("lower", {GetInt4RangeType()}, LogicalType::INTEGER, RangeLower);
	loader.RegisterFunction(lower_fun);

	// Accessor: upper(INT4RANGE) -> INTEGER
	ScalarFunction upper_fun("upper", {GetInt4RangeType()}, LogicalType::INTEGER, RangeUpper);
	loader.RegisterFunction(upper_fun);

	// Accessor: isempty(INT4RANGE) -> BOOLEAN
	ScalarFunction isempty_fun("isempty", {GetInt4RangeType()}, LogicalType::BOOLEAN, RangeIsEmpty);
	loader.RegisterFunction(isempty_fun);

	// Accessor: lower_inc(INT4RANGE) -> BOOLEAN
	ScalarFunction lower_inc_fun("lower_inc", {GetInt4RangeType()}, LogicalType::BOOLEAN, RangeLowerInc);
	loader.RegisterFunction(lower_inc_fun);

	// Accessor: upper_inc(INT4RANGE) -> BOOLEAN
	ScalarFunction upper_inc_fun("upper_inc", {GetInt4RangeType()}, LogicalType::BOOLEAN, RangeUpperInc);
	loader.RegisterFunction(upper_inc_fun);
}

void RangesExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string RangesExtension::Name() {
	return "ranges";
}

std::string RangesExtension::Version() const {
#ifdef EXT_VERSION_RANGES
	return EXT_VERSION_RANGES;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ranges, loader) {
	duckdb::LoadInternal(loader);
}
}
