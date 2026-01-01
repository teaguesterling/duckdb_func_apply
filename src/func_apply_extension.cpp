#define DUCKDB_EXTENSION_MAIN

#include "func_apply_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void FuncApplyScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "FuncApply " + name.GetString() + " 🐥");
	});
}

inline void FuncApplyOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "FuncApply " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

//===--------------------------------------------------------------------===//
// function_exists(name VARCHAR) -> BOOLEAN
//===--------------------------------------------------------------------===//

static bool CheckFunctionExists(ClientContext &context, const string &func_name) {
	if (func_name.empty()) {
		return false;
	}

	auto &catalog = Catalog::GetSystemCatalog(context);

	// Check each function type in order
	static const vector<CatalogType> function_types = {
	    CatalogType::SCALAR_FUNCTION_ENTRY,
	    CatalogType::AGGREGATE_FUNCTION_ENTRY,
	    CatalogType::TABLE_FUNCTION_ENTRY,
	    CatalogType::MACRO_ENTRY
	};

	for (auto type : function_types) {
		auto entry = catalog.GetEntry(context, type, DEFAULT_SCHEMA, func_name,
		                              OnEntryNotFound::RETURN_NULL);
		if (entry) {
			return true;
		}
	}
	return false;
}

inline void FunctionExistsScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &name_vector = args.data[0];

	UnaryExecutor::Execute<string_t, bool>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
		    return CheckFunctionExists(context, name.GetString());
	    }
	);
}

//===--------------------------------------------------------------------===//
// apply() and apply_with() helper functions
//===--------------------------------------------------------------------===//

// Validate that a string is a valid SQL identifier (prevents injection)
static bool IsValidIdentifier(const string &name) {
	if (name.empty()) {
		return false;
	}
	// First character must be letter or underscore
	if (!std::isalpha(static_cast<unsigned char>(name[0])) && name[0] != '_') {
		return false;
	}
	// Rest must be alphanumeric or underscore
	for (size_t i = 1; i < name.size(); i++) {
		char c = name[i];
		if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') {
			return false;
		}
	}
	return true;
}

// Convert a DuckDB Value to a SQL literal string with proper escaping
static string ValueToSQL(const Value &val) {
	if (val.IsNull()) {
		return "NULL";
	}

	switch (val.type().id()) {
	case LogicalTypeId::VARCHAR: {
		// Escape single quotes by doubling them
		string str = val.ToString();
		string escaped;
		escaped.reserve(str.size() + 2);
		escaped += '\'';
		for (char c : str) {
			if (c == '\'') {
				escaped += "''";
			} else {
				escaped += c;
			}
		}
		escaped += '\'';
		return escaped;
	}
	case LogicalTypeId::BLOB: {
		return "'" + val.ToString() + "'::BLOB";
	}
	case LogicalTypeId::LIST: {
		string result = "[";
		auto &children = ListValue::GetChildren(val);
		for (size_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += ValueToSQL(children[i]);
		}
		result += "]";
		return result;
	}
	case LogicalTypeId::STRUCT: {
		string result = "{";
		auto &children = StructValue::GetChildren(val);
		auto &type = val.type();
		for (size_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += "'" + StructType::GetChildName(type, i) + "': ";
			result += ValueToSQL(children[i]);
		}
		result += "}";
		return result;
	}
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		// Numeric types can be used directly
		return val.ToString();
	case LogicalTypeId::DATE:
		return "'" + val.ToString() + "'::DATE";
	case LogicalTypeId::TIME:
		return "'" + val.ToString() + "'::TIME";
	case LogicalTypeId::TIMESTAMP:
		return "'" + val.ToString() + "'::TIMESTAMP";
	case LogicalTypeId::INTERVAL:
		return "'" + val.ToString() + "'::INTERVAL";
	default:
		// For other types, try to use as string literal with cast
		return "'" + StringUtil::Replace(val.ToString(), "'", "''") + "'::" + val.type().ToString();
	}
}

// Helper to determine the catalog type of a function
// Uses cross-catalog search to find functions in any catalog (system, temp, user-defined)
static CatalogType GetFunctionCatalogType(ClientContext &context, const string &func_name) {
	// Check in order of likelihood
	static const vector<CatalogType> function_types = {
	    CatalogType::SCALAR_FUNCTION_ENTRY,
	    CatalogType::MACRO_ENTRY,
	    CatalogType::AGGREGATE_FUNCTION_ENTRY,
	    CatalogType::TABLE_FUNCTION_ENTRY
	};

	for (auto type : function_types) {
		// Use static Catalog::GetEntry which searches across all catalogs
		EntryLookupInfo lookup_info(type, func_name);
		auto entry = Catalog::GetEntry(context, INVALID_CATALOG, DEFAULT_SCHEMA, lookup_info,
		                               OnEntryNotFound::RETURN_NULL);
		if (entry) {
			return entry->type;
		}
	}
	return CatalogType::INVALID;
}

// Execute a function by name with given argument values
// Uses expression-based execution to avoid query planner deadlock
// Handles both scalar functions and macros
static Value ExecuteFunction(ClientContext &context, const string &func_name, const vector<Value> &args) {
	// Check the function type first
	auto func_type = GetFunctionCatalogType(context, func_name);

	if (func_type == CatalogType::INVALID) {
		throw InvalidInputException("Function '%s' does not exist", func_name);
	}

	if (func_type == CatalogType::SCALAR_FUNCTION_ENTRY) {
		// For scalar functions, use FunctionBinder directly (fast path)
		vector<unique_ptr<Expression>> arg_exprs;
		for (auto &arg : args) {
			arg_exprs.push_back(make_uniq<BoundConstantExpression>(arg));
		}

		ErrorData error;
		FunctionBinder binder(context);
		auto bound_expr = binder.BindScalarFunction(DEFAULT_SCHEMA, func_name, std::move(arg_exprs), error);

		if (error.HasError()) {
			throw InvalidInputException("Function '%s': %s", func_name, error.Message());
		}

		if (!bound_expr) {
			throw InvalidInputException("Function '%s' binding failed", func_name);
		}

		return ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
	}

	if (func_type == CatalogType::MACRO_ENTRY) {
		// For macros, we need to use the full expression binding path
		// Create a parsed FunctionExpression and bind it through ConstantBinder
		vector<unique_ptr<ParsedExpression>> parsed_args;
		for (auto &arg : args) {
			parsed_args.push_back(make_uniq<ConstantExpression>(arg));
		}

		unique_ptr<ParsedExpression> func_expr = make_uniq<FunctionExpression>(func_name, std::move(parsed_args));

		// Create a binder and use ConstantBinder to bind the expression
		auto binder = Binder::CreateBinder(context);
		ConstantBinder constant_binder(*binder, context, "apply");
		auto bind_result = constant_binder.Bind(func_expr);

		return ExpressionExecutor::EvaluateScalar(context, *bind_result, true);
	}

	// For other types (aggregates, table functions), throw an error
	throw InvalidInputException("Function '%s' is not a scalar function or macro", func_name);
}

//===--------------------------------------------------------------------===//
// apply(func VARCHAR, ...args ANY) -> ANY
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> BindApply(ClientContext &context, ScalarFunction &bound_function,
                                          vector<unique_ptr<Expression>> &arguments) {
	// Default return type
	bound_function.return_type = LogicalType::VARCHAR;

	// If no arguments beyond function name, nothing to infer
	if (arguments.empty()) {
		return nullptr;
	}

	// Check if function name is a compile-time constant
	if (!arguments[0]->IsFoldable()) {
		return nullptr;
	}

	// Evaluate to get function name
	Value func_name_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (func_name_val.IsNull()) {
		return nullptr;
	}
	string func_name = StringValue::Get(func_name_val);

	// Validate function name
	if (!IsValidIdentifier(func_name)) {
		return nullptr;
	}

	// Check the function type
	auto func_type = GetFunctionCatalogType(context, func_name);

	if (func_type == CatalogType::SCALAR_FUNCTION_ENTRY) {
		// For scalar functions, use FunctionBinder directly
		vector<unique_ptr<Expression>> target_args;
		for (idx_t i = 1; i < arguments.size(); i++) {
			target_args.push_back(arguments[i]->Copy());
		}

		ErrorData error;
		FunctionBinder binder(context);
		auto bound_expr = binder.BindScalarFunction(DEFAULT_SCHEMA, func_name, std::move(target_args), error);

		if (error.HasError() || !bound_expr) {
			return nullptr;
		}

		bound_function.return_type = bound_expr->return_type;
		return nullptr;
	}

	if (func_type == CatalogType::MACRO_ENTRY) {
		// For macros, use the full expression binding path
		vector<unique_ptr<ParsedExpression>> parsed_args;
		for (idx_t i = 1; i < arguments.size(); i++) {
			// Try to evaluate constant expressions, otherwise create a placeholder
			if (arguments[i]->IsFoldable()) {
				auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[i]);
				parsed_args.push_back(make_uniq<ConstantExpression>(val));
			} else {
				// For non-constant expressions, we need to use the expression's type
				// Create a constant with a dummy value of the right type
				parsed_args.push_back(make_uniq<ConstantExpression>(Value(arguments[i]->return_type)));
			}
		}

		unique_ptr<ParsedExpression> func_expr = make_uniq<FunctionExpression>(func_name, std::move(parsed_args));

		try {
			auto binder = Binder::CreateBinder(context);
			ConstantBinder constant_binder(*binder, context, "apply");
			auto bound_expr = constant_binder.Bind(func_expr);
			bound_function.return_type = bound_expr->return_type;
		} catch (...) {
			// If binding fails, fall back to VARCHAR
			return nullptr;
		}

		return nullptr;
	}

	// For other types, keep default VARCHAR
	return nullptr;
}

static void ApplyScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	idx_t count = args.size();

	for (idx_t i = 0; i < count; i++) {
		// Get function name
		auto func_name_val = args.data[0].GetValue(i);
		if (func_name_val.IsNull()) {
			result.SetValue(i, Value());
			continue;
		}

		string func_name = StringValue::Get(func_name_val);

		// Validate function name
		if (!IsValidIdentifier(func_name)) {
			throw InvalidInputException("apply: invalid function name '%s'", func_name);
		}

		// Collect arguments
		vector<Value> func_args;
		for (idx_t j = 1; j < args.ColumnCount(); j++) {
			func_args.push_back(args.data[j].GetValue(i));
		}

		// Execute the function
		try {
			auto val = ExecuteFunction(context, func_name, func_args);
			result.SetValue(i, val);
		} catch (const Exception &e) {
			throw InvalidInputException("apply('%s'): %s", func_name, e.what());
		}
	}
}

//===--------------------------------------------------------------------===//
// apply_with(func VARCHAR, args LIST, kwargs STRUCT) -> ANY
//===--------------------------------------------------------------------===//

// Bind data for apply_with - stores which columns are args vs kwargs
struct ApplyWithBindData : public FunctionData {
	idx_t args_idx = 1;      // Column index for args (default: second arg)
	idx_t kwargs_idx = 2;    // Column index for kwargs (default: third arg)
	bool has_kwargs = false; // Whether kwargs was provided

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ApplyWithBindData>();
		result->args_idx = args_idx;
		result->kwargs_idx = kwargs_idx;
		result->has_kwargs = has_kwargs;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<ApplyWithBindData>();
		return args_idx == o.args_idx && kwargs_idx == o.kwargs_idx && has_kwargs == o.has_kwargs;
	}
};

static unique_ptr<FunctionData> BindApplyWith(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = make_uniq<ApplyWithBindData>();
	bound_function.return_type = LogicalType::VARCHAR;

	if (arguments.empty()) {
		throw InvalidInputException("apply_with requires at least a function name");
	}

	// First argument is always the function name
	// Remaining arguments can be positional (args, kwargs) or named (args := ..., kwargs := ...)
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &alias = arguments[i]->alias;
		if (alias == "args") {
			bind_data->args_idx = i;
		} else if (alias == "kwargs") {
			bind_data->kwargs_idx = i;
			bind_data->has_kwargs = true;
		} else if (i == 1) {
			// Positional: first extra arg is args
			bind_data->args_idx = i;
		} else if (i == 2) {
			// Positional: second extra arg is kwargs
			bind_data->kwargs_idx = i;
			bind_data->has_kwargs = true;
		}
	}

	// Try to infer return type if function name is constant
	if (arguments[0]->IsFoldable()) {
		Value func_name_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
		if (!func_name_val.IsNull()) {
			string func_name = StringValue::Get(func_name_val);
			if (IsValidIdentifier(func_name)) {
				auto func_type = GetFunctionCatalogType(context, func_name);
				if (func_type == CatalogType::SCALAR_FUNCTION_ENTRY) {
					auto &catalog = Catalog::GetSystemCatalog(context);
					auto func_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
					    context, DEFAULT_SCHEMA, func_name, OnEntryNotFound::RETURN_NULL);
					if (func_entry && !func_entry->functions.functions.empty()) {
						auto &first_func = func_entry->functions.functions[0];
						if (first_func.return_type.id() != LogicalTypeId::ANY) {
							bound_function.return_type = first_func.return_type;
						}
					}
				}
			}
		}
	}

	return std::move(bind_data);
}

static void ApplyWithScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<ApplyWithBindData>();
	idx_t count = args.size();

	for (idx_t i = 0; i < count; i++) {
		// Get function name
		auto func_name_val = args.data[0].GetValue(i);
		if (func_name_val.IsNull()) {
			result.SetValue(i, Value());
			continue;
		}

		string func_name = StringValue::Get(func_name_val);

		// Validate function name
		if (!IsValidIdentifier(func_name)) {
			throw InvalidInputException("apply_with: invalid function name '%s'", func_name);
		}

		// Get args list
		Value args_list;
		if (bind_data.args_idx < args.ColumnCount()) {
			args_list = args.data[bind_data.args_idx].GetValue(i);
		}

		// Collect positional args from list
		vector<Value> func_args;
		if (!args_list.IsNull() && args_list.type().id() == LogicalTypeId::LIST) {
			auto &list_children = ListValue::GetChildren(args_list);
			for (auto &child : list_children) {
				func_args.push_back(child);
			}
		}

		// Get kwargs if provided
		if (bind_data.has_kwargs && bind_data.kwargs_idx < args.ColumnCount()) {
			auto kwargs_struct = args.data[bind_data.kwargs_idx].GetValue(i);
			if (!kwargs_struct.IsNull() && kwargs_struct.type().id() == LogicalTypeId::STRUCT) {
				auto &struct_children = StructValue::GetChildren(kwargs_struct);
				if (!struct_children.empty()) {
					throw InvalidInputException(
					    "apply_with: kwargs (named parameters) are not yet supported. "
					    "Use positional args instead.");
				}
			}
		}

		// Execute the function
		try {
			auto val = ExecuteFunction(context, func_name, func_args);
			result.SetValue(i, val);
		} catch (const Exception &e) {
			throw InvalidInputException("apply_with('%s'): %s", func_name, e.what());
		}
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto func_apply_scalar_function = ScalarFunction("func_apply", {LogicalType::VARCHAR}, LogicalType::VARCHAR, FuncApplyScalarFun);
	loader.RegisterFunction(func_apply_scalar_function);

	// Register another scalar function
	auto func_apply_openssl_version_scalar_function = ScalarFunction("func_apply_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, FuncApplyOpenSSLVersionScalarFun);
	loader.RegisterFunction(func_apply_openssl_version_scalar_function);

	// Register function_exists
	auto function_exists_func = ScalarFunction("function_exists", {LogicalType::VARCHAR},
	                                           LogicalType::BOOLEAN, FunctionExistsScalarFun);
	loader.RegisterFunction(function_exists_func);

	// Register apply (variadic)
	auto apply_func = ScalarFunction("apply", {LogicalType::VARCHAR}, LogicalType::ANY, ApplyScalarFun,
	                                 BindApply);
	apply_func.varargs = LogicalType::ANY;
	apply_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(apply_func);

	// Register apply_with (structured with named params support)
	// Uses varargs to support: apply_with(func, args) or apply_with(func, args, kwargs)
	// or named: apply_with(func, args := [...], kwargs := {...})
	auto apply_with_func = ScalarFunction("apply_with",
	                                       {LogicalType::VARCHAR},
	                                       LogicalType::ANY, ApplyWithScalarFun, BindApplyWith);
	apply_with_func.varargs = LogicalType::ANY;
	apply_with_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(apply_with_func);
}

void FuncApplyExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string FuncApplyExtension::Name() {
	return "func_apply";
}

std::string FuncApplyExtension::Version() const {
#ifdef EXT_VERSION_FUNC_APPLY
	return EXT_VERSION_FUNC_APPLY;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(func_apply, loader) {
	duckdb::LoadInternal(loader);
}
}
