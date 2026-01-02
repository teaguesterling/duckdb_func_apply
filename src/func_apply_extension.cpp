#define DUCKDB_EXTENSION_MAIN

//===--------------------------------------------------------------------===//
// func_apply Extension - Dynamic Function Invocation for DuckDB
//===--------------------------------------------------------------------===//
//
// This extension provides dynamic function invocation capabilities:
//
// SCALAR FUNCTIONS:
//   - apply(func, ...args) - Call a scalar function or macro by name
//   - apply_with(func, args := [...], kwargs := {...}) - Structured call
//   - function_exists(func) - Check if a function exists
//
// TABLE FUNCTIONS:
//   - apply_table(func, ...args) - Call a table function by name
//   - apply_table_with(func, args := [...], kwargs := {...}) - Structured call
//
//===--------------------------------------------------------------------===//
// IMPORTANT IMPLEMENTATION NOTES FOR FUTURE DEVELOPERS
//===--------------------------------------------------------------------===//
//
// 1. DUCKDB CATALOG API QUIRK:
//    DuckDB's catalog.GetEntry(context, type, schema, name, ...) does NOT
//    filter by CatalogType! It returns any entry matching the name regardless
//    of type. You MUST verify entry->type matches the requested type yourself.
//
//    The EntryLookupInfo API DOES check type but throws an exception on
//    mismatch instead of returning null.
//
//    See FunctionExistsOfType() for the correct pattern.
//
// 2. FUNCTION TYPE HIERARCHY:
//    DuckDB has several function types that "functions" can be:
//    - SCALAR_FUNCTION_ENTRY: Native scalar functions (upper, abs, etc.)
//    - MACRO_ENTRY: SQL macros (list_sum, etc. - many "functions" are macros!)
//    - TABLE_FUNCTION_ENTRY: Table functions (range, read_csv, etc.)
//    - AGGREGATE_FUNCTION_ENTRY: Aggregate functions (sum, count, etc.)
//
//    Many functions that seem like scalar functions are actually macros.
//    For example, list_sum is a MACRO, not a SCALAR_FUNCTION.
//
// 3. FUNCTION TYPE CHECKING ORDER:
//    When looking up a function by name, the order of type checks matters.
//    Some functions (like 'range') exist as both scalar AND table functions.
//    GetCallableFunctionType() checks SCALAR first, then MACRO.
//
// 4. BIND vs EXECUTE PATHS:
//    - Scalar functions: Use FunctionBinder directly (fast, avoids deadlock)
//    - Macros: Must use full expression binding via ConstantBinder
//    - Table functions: Use bind_replace to generate SQL dynamically
//
//===--------------------------------------------------------------------===//

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
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"

#include <unordered_set>
#include <mutex>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Security Configuration
//===--------------------------------------------------------------------===//
//
// Configurable security model with three modes:
// - none: No restrictions (default)
// - blacklist: Block specific functions (with sensible defaults)
// - whitelist: Only allow specific functions
// - validator: Call a custom function/macro to validate calls
//
// Configuration via SET statements:
//   SET func_apply_security_mode = 'blacklist';
//   SET func_apply_blacklist = ['system', 'load'];
//   SET func_apply_security_locked = true;  -- One-way lock
//

// Default blacklist of dangerous functions
static const vector<string> DEFAULT_BLACKLIST = {
    // Extension management
    "load", "install", "uninstall", "force_install",
    // System access
    "system", "getenv",
    // File operations that could be dangerous
    "export_database", "import_database",
    // Secret management
    "create_secret", "drop_secret"
};

// Security configuration per session
struct FuncApplySecurityConfig {
	// Mode: "none", "blacklist", "whitelist", "validator"
	string mode = "none";

	// Blacklist of functions to block (used when mode = "blacklist")
	unordered_set<string> blacklist;

	// Whitelist of allowed functions (used when mode = "whitelist")
	unordered_set<string> whitelist;

	// Validator function name (used when mode = "validator")
	string validator_func;

	// Block behavior: "error", "null", "default"
	string on_block = "error";

	// Default value to return when blocked (used when on_block = "default")
	Value block_default;

	// Lock state - once true, cannot be changed
	bool locked = false;

	// Initialize with default blacklist
	FuncApplySecurityConfig() {
		for (const auto &func : DEFAULT_BLACKLIST) {
			blacklist.insert(StringUtil::Lower(func));
		}
	}
};

// Global map for per-session security configuration
// Key: raw pointer to ClientContext (lifetime managed by DuckDB)
static mutex security_config_mutex;
static unordered_map<ClientContext*, unique_ptr<FuncApplySecurityConfig>> security_configs;

// Get or create security config for a session
static FuncApplySecurityConfig &GetSecurityConfig(ClientContext &context) {
	lock_guard<mutex> lock(security_config_mutex);
	auto it = security_configs.find(&context);
	if (it == security_configs.end()) {
		auto config = make_uniq<FuncApplySecurityConfig>();
		auto &ref = *config;
		security_configs[&context] = std::move(config);
		return ref;
	}
	return *it->second;
}

// Clean up security config when session ends (called from destructor or explicitly)
static void CleanupSecurityConfig(ClientContext &context) {
	lock_guard<mutex> lock(security_config_mutex);
	security_configs.erase(&context);
}

// Forward declaration for validator
static Value ExecuteFunctionInternal(ClientContext &context, const string &func_name,
                                     const vector<Value> &args, bool skip_security_check);

// Call the validator function to check if a call is allowed
// Builds a parameters struct with the following structure:
// {
//   total_args: INTEGER,
//   positional: {
//     arg_indexes: VARCHAR[],  -- ['1', '2', '3', ...]
//     arg_types: VARCHAR[],    -- ['VARCHAR', 'INTEGER', ...]
//     arg_values: STRUCT       -- {'1': val1, '2': val2, ...}
//   },
//   named: {
//     arg_names: VARCHAR[],    -- ['start', 'length', ...]
//     arg_types: VARCHAR[],    -- ['INTEGER', 'INTEGER', ...]
//     arg_values: STRUCT       -- {'start': 7, 'length': 5, ...}
//   }
// }
static bool CallValidator(ClientContext &context, const string &validator_name,
                          const string &func_name, const vector<Value> &positional_args,
                          const case_insensitive_map_t<Value> &named_args) {
	// Build positional struct
	vector<Value> pos_indexes;
	vector<Value> pos_types;
	child_list_t<Value> pos_values;
	for (size_t i = 0; i < positional_args.size(); i++) {
		string idx = to_string(i + 1);
		pos_indexes.push_back(Value(idx));
		pos_types.push_back(Value(positional_args[i].type().ToString()));
		pos_values.push_back(make_pair(idx, positional_args[i]));
	}

	Value positional_struct;
	if (pos_values.empty()) {
		// Empty positional - need to create struct with empty arrays and empty struct
		child_list_t<Value> empty_struct_values;
		child_list_t<Value> pos_struct_fields;
		pos_struct_fields.push_back(make_pair("arg_indexes", Value::LIST(LogicalType::VARCHAR, vector<Value>())));
		pos_struct_fields.push_back(make_pair("arg_types", Value::LIST(LogicalType::VARCHAR, vector<Value>())));
		pos_struct_fields.push_back(make_pair("arg_values", Value::STRUCT(std::move(empty_struct_values))));
		positional_struct = Value::STRUCT(std::move(pos_struct_fields));
	} else {
		child_list_t<Value> pos_struct_fields;
		pos_struct_fields.push_back(make_pair("arg_indexes", Value::LIST(LogicalType::VARCHAR, std::move(pos_indexes))));
		pos_struct_fields.push_back(make_pair("arg_types", Value::LIST(LogicalType::VARCHAR, std::move(pos_types))));
		pos_struct_fields.push_back(make_pair("arg_values", Value::STRUCT(std::move(pos_values))));
		positional_struct = Value::STRUCT(std::move(pos_struct_fields));
	}

	// Build named struct
	vector<Value> named_names;
	vector<Value> named_types;
	child_list_t<Value> named_values;
	for (auto &kv : named_args) {
		named_names.push_back(Value(kv.first));
		named_types.push_back(Value(kv.second.type().ToString()));
		named_values.push_back(make_pair(kv.first, kv.second));
	}

	Value named_struct;
	if (named_values.empty()) {
		// Empty named - need to create struct with empty arrays and empty struct
		child_list_t<Value> empty_struct_values;
		child_list_t<Value> named_struct_fields;
		named_struct_fields.push_back(make_pair("arg_names", Value::LIST(LogicalType::VARCHAR, vector<Value>())));
		named_struct_fields.push_back(make_pair("arg_types", Value::LIST(LogicalType::VARCHAR, vector<Value>())));
		named_struct_fields.push_back(make_pair("arg_values", Value::STRUCT(std::move(empty_struct_values))));
		named_struct = Value::STRUCT(std::move(named_struct_fields));
	} else {
		child_list_t<Value> named_struct_fields;
		named_struct_fields.push_back(make_pair("arg_names", Value::LIST(LogicalType::VARCHAR, std::move(named_names))));
		named_struct_fields.push_back(make_pair("arg_types", Value::LIST(LogicalType::VARCHAR, std::move(named_types))));
		named_struct_fields.push_back(make_pair("arg_values", Value::STRUCT(std::move(named_values))));
		named_struct = Value::STRUCT(std::move(named_struct_fields));
	}

	// Build the top-level parameters struct
	int32_t total_args = static_cast<int32_t>(positional_args.size() + named_args.size());
	child_list_t<Value> params_fields;
	params_fields.push_back(make_pair("total_args", Value::INTEGER(total_args)));
	params_fields.push_back(make_pair("positional", std::move(positional_struct)));
	params_fields.push_back(make_pair("named", std::move(named_struct)));
	Value parameters = Value::STRUCT(std::move(params_fields));

	// Call validator function (skip security check to avoid infinite recursion)
	vector<Value> validator_args = {Value(func_name), parameters};
	try {
		Value result = ExecuteFunctionInternal(context, validator_name, validator_args, true);
		if (result.IsNull()) {
			return false;
		}
		return BooleanValue::Get(result);
	} catch (std::exception &e) {
		// Re-throw with context for debugging
		throw InvalidInputException("Validator '%s' failed: %s", validator_name, e.what());
	}
}

// Validate a function call against the security policy
// Returns true if allowed, false if blocked (caller handles on_block behavior)
// Throws if on_block = "error" and the call is blocked
static bool ValidateFunctionCall(ClientContext &context, const string &func_name,
                                 const vector<Value> &positional_args,
                                 const case_insensitive_map_t<Value> &named_args = {}) {
	auto &config = GetSecurityConfig(context);

	// No restrictions in "none" mode
	if (config.mode == "none") {
		return true;
	}

	bool allowed = false;
	string lower_name = StringUtil::Lower(func_name);

	if (config.mode == "blacklist") {
		// Allowed if NOT in blacklist
		allowed = config.blacklist.find(lower_name) == config.blacklist.end();
	} else if (config.mode == "whitelist") {
		// Allowed if IN whitelist
		allowed = config.whitelist.find(lower_name) != config.whitelist.end();
	} else if (config.mode == "validator") {
		// Call the validator function
		if (config.validator_func.empty()) {
			throw InvalidInputException("func_apply: validator mode enabled but no validator function set");
		}
		allowed = CallValidator(context, config.validator_func, func_name, positional_args, named_args);
	}

	if (!allowed) {
		if (config.on_block == "error") {
			throw InvalidInputException(
			    "Function '%s' is blocked by func_apply security policy (mode: %s)",
			    func_name, config.mode);
		}
		return false;
	}

	return true;
}

// Get the blocked return value based on on_block setting
static Value GetBlockedValue(ClientContext &context) {
	auto &config = GetSecurityConfig(context);
	if (config.on_block == "null") {
		return Value();
	} else if (config.on_block == "default") {
		return config.block_default;
	}
	// Should not reach here if ValidateFunctionCall threw
	return Value();
}

//===--------------------------------------------------------------------===//
// function_exists(name VARCHAR) -> BOOLEAN
//===--------------------------------------------------------------------===//

// Helper to check if a function exists in a specific catalog
static bool CheckFunctionExistsInCatalog(ClientContext &context, Catalog &catalog,
                                         const string &func_name, const vector<CatalogType> &types) {
	for (auto type : types) {
		auto entry = catalog.GetEntry(context, type, DEFAULT_SCHEMA, func_name,
		                              OnEntryNotFound::RETURN_NULL);
		if (entry) {
			return true;
		}
	}
	return false;
}

static bool CheckFunctionExists(ClientContext &context, const string &func_name) {
	if (func_name.empty()) {
		return false;
	}

	// Check each function type in order
	static const vector<CatalogType> function_types = {
	    CatalogType::SCALAR_FUNCTION_ENTRY,
	    CatalogType::AGGREGATE_FUNCTION_ENTRY,
	    CatalogType::TABLE_FUNCTION_ENTRY,
	    CatalogType::MACRO_ENTRY
	};

	// First check system catalog (built-in functions)
	auto &system_catalog = Catalog::GetSystemCatalog(context);
	if (CheckFunctionExistsInCatalog(context, system_catalog, func_name, function_types)) {
		return true;
	}

	// Also check the default database catalog (user-defined functions/macros)
	auto &db_manager = DatabaseManager::Get(context);
	auto default_db_name = db_manager.GetDefaultDatabase(context);
	if (!default_db_name.empty()) {
		auto catalog_entry = Catalog::GetCatalogEntry(context, default_db_name);
		if (catalog_entry) {
			if (CheckFunctionExistsInCatalog(context, *catalog_entry, func_name, function_types)) {
				return true;
			}
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

// Helper to check if a function of a specific type exists
//
// IMPORTANT DISCOVERY: DuckDB's catalog.GetEntry(context, type, schema, name, ...)
// does NOT filter by type! It returns any entry with that name regardless of type.
// We MUST verify entry->type matches the requested type ourselves.
//
// The EntryLookupInfo API (Catalog::GetEntry with EntryLookupInfo) DOES check type
// but throws an exception on mismatch instead of returning null.
//
// Neither API does what we want (return null on type mismatch), so we use the
// non-throwing version and add our own type check.
static bool FunctionExistsOfType(ClientContext &context, const string &func_name, CatalogType type) {
	// First check system catalog (built-in functions)
	auto &system_catalog = Catalog::GetSystemCatalog(context);
	auto entry = system_catalog.GetEntry(context, type, DEFAULT_SCHEMA, func_name, OnEntryNotFound::RETURN_NULL);

	if (entry && entry->type == type) {
		return true;
	}

	// Also check the default database catalog (user-defined functions/macros)
	auto &db_manager = DatabaseManager::Get(context);
	auto default_db_name = db_manager.GetDefaultDatabase(context);
	if (!default_db_name.empty()) {
		auto catalog_entry = Catalog::GetCatalogEntry(context, default_db_name);
		if (catalog_entry) {
			auto user_entry = catalog_entry->GetEntry(context, type, DEFAULT_SCHEMA, func_name,
			                                          OnEntryNotFound::RETURN_NULL);
			if (user_entry && user_entry->type == type) {
				return true;
			}
		}
	}

	return false;
}

// Helper to find what type of callable function exists (for apply/apply_with)
// Returns the type of the first matching callable (scalar or macro), or INVALID if not found
// This specifically excludes table functions since those require apply_table
//
// NOTE: Order matters - we check SCALAR first, then MACRO. This means if a function
// exists as both (rare), we prefer the scalar version.
static CatalogType GetCallableFunctionType(ClientContext &context, const string &func_name) {
	// Order matters: prefer scalar functions, then macros
	// We exclude table functions - those must be called via apply_table
	static const vector<CatalogType> callable_types = {
	    CatalogType::SCALAR_FUNCTION_ENTRY,
	    CatalogType::MACRO_ENTRY
	};

	for (auto type : callable_types) {
		if (FunctionExistsOfType(context, func_name, type)) {
			return type;
		}
	}
	return CatalogType::INVALID;
}

// Helper to check if a table function exists (for apply_table/apply_table_with)
static bool TableFunctionExists(ClientContext &context, const string &func_name) {
	return FunctionExistsOfType(context, func_name, CatalogType::TABLE_FUNCTION_ENTRY);
}

// Execute a function by name with given argument values (internal version)
// Uses expression-based execution to avoid query planner deadlock
// Handles both scalar functions and macros
//
// NOTE: This is called at runtime for each row. The function type was already
// determined at bind time in BindApply, but we re-check here because the function
// name could be dynamic (coming from a column value).
//
// skip_security_check: Set to true when calling validator functions to avoid infinite recursion
static Value ExecuteFunctionInternal(ClientContext &context, const string &func_name,
                                     const vector<Value> &args, bool skip_security_check) {
	// Security check (unless skipped for validator calls)
	if (!skip_security_check) {
		if (!ValidateFunctionCall(context, func_name, args)) {
			// Function is blocked, return the configured blocked value
			return GetBlockedValue(context);
		}
	}

	// Check the function type first (only scalar functions and macros are callable)
	auto func_type = GetCallableFunctionType(context, func_name);

	if (func_type == CatalogType::INVALID) {
		// Check if it's a table function to give a better error message
		if (TableFunctionExists(context, func_name)) {
			throw InvalidInputException("Function '%s' is a table function. Use apply_table() instead.", func_name);
		}
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
		// Macros are SQL expressions that get expanded, so we can't use FunctionBinder.
		// Instead, create a parsed FunctionExpression and bind it through ConstantBinder.
		vector<unique_ptr<ParsedExpression>> parsed_args;
		for (auto &arg : args) {
			parsed_args.push_back(make_uniq<ConstantExpression>(arg));
		}

		unique_ptr<ParsedExpression> func_expr = make_uniq<FunctionExpression>(func_name, std::move(parsed_args));

		// Create a binder and use ConstantBinder to bind the expression
		// ConstantBinder is designed for binding expressions in a constant context
		auto binder = Binder::CreateBinder(context);
		ConstantBinder constant_binder(*binder, context, "apply");
		auto bind_result = constant_binder.Bind(func_expr);

		return ExpressionExecutor::EvaluateScalar(context, *bind_result, true);
	}

	// For other types (aggregates, table functions), throw an error
	// This shouldn't happen if GetCallableFunctionType works correctly
	throw InvalidInputException("Function '%s' is not a scalar function or macro", func_name);
}

// Public version that always performs security check
static Value ExecuteFunction(ClientContext &context, const string &func_name, const vector<Value> &args) {
	return ExecuteFunctionInternal(context, func_name, args, false);
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

	// Check the function type (only scalar functions and macros are callable via apply)
	auto func_type = GetCallableFunctionType(context, func_name);

	if (func_type == CatalogType::SCALAR_FUNCTION_ENTRY) {
		// For scalar functions, use FunctionBinder directly
		// FunctionBinder handles overload resolution and type coercion automatically
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
				auto func_type = GetCallableFunctionType(context, func_name);
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

//===--------------------------------------------------------------------===//
// apply_table(func VARCHAR, ...args ANY) -> TABLE
//===--------------------------------------------------------------------===//

// Helper function to parse a query into a SubqueryRef
static unique_ptr<SubqueryRef> ParseSubquery(const string &query, const ParserOptions &options) {
	Parser parser(options);
	parser.ParseQuery(query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw BinderException("apply_table: expected a single SELECT statement from generated query");
	}
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	return make_uniq<SubqueryRef>(std::move(select_stmt));
}

// bind_replace for apply_table: generates SQL and replaces with subquery
static unique_ptr<TableRef> ApplyTableBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	// First argument is the function name
	if (input.inputs.empty()) {
		throw BinderException("apply_table requires at least a function name");
	}

	auto &func_name_val = input.inputs[0];
	if (func_name_val.IsNull()) {
		throw BinderException("apply_table: function name cannot be NULL");
	}

	string func_name = StringValue::Get(func_name_val);

	// Validate function name
	if (!IsValidIdentifier(func_name)) {
		throw BinderException("apply_table: invalid function name '%s'", func_name);
	}

	// Security check - collect args for validation
	vector<Value> args_for_validation;
	for (idx_t i = 1; i < input.inputs.size(); i++) {
		args_for_validation.push_back(input.inputs[i]);
	}

	// Validate against security policy (will throw if on_block = "error")
	if (!ValidateFunctionCall(context, func_name, args_for_validation)) {
		// If we get here, on_block is "null" or "default" - but table functions
		// can't return those, so we throw a specific error
		throw BinderException("apply_table: function '%s' is blocked by security policy", func_name);
	}

	// Check if it's a table function
	if (!TableFunctionExists(context, func_name)) {
		// Check if it's a scalar function to give a better error message
		if (GetCallableFunctionType(context, func_name) != CatalogType::INVALID) {
			throw BinderException("apply_table: '%s' is a scalar function. Use apply() instead.", func_name);
		}
		throw BinderException("apply_table: function '%s' does not exist", func_name);
	}

	// Build the SQL query: SELECT * FROM func_name(arg1, arg2, ...)
	string sql = "SELECT * FROM " + func_name + "(";
	for (idx_t i = 1; i < input.inputs.size(); i++) {
		if (i > 1) {
			sql += ", ";
		}
		sql += ValueToSQL(input.inputs[i]);
	}
	sql += ")";

	// Add named parameters if any
	if (!input.named_parameters.empty()) {
		// Named parameters need to be added after regular arguments
		bool first_named = input.inputs.size() <= 1;
		for (auto &kv : input.named_parameters) {
			if (!first_named || input.inputs.size() > 1) {
				sql = sql.substr(0, sql.length() - 1); // Remove closing paren
				sql += ", ";
			} else {
				sql = sql.substr(0, sql.length() - 1); // Remove closing paren
			}
			sql += kv.first + " := " + ValueToSQL(kv.second) + ")";
			first_named = false;
		}
	}

	// Parse and return as subquery
	return ParseSubquery(sql, context.GetParserOptions());
}

//===--------------------------------------------------------------------===//
// apply_table_with(func VARCHAR, args LIST, kwargs STRUCT) -> TABLE
//===--------------------------------------------------------------------===//

// bind_replace for apply_table_with: generates SQL and replaces with subquery
static unique_ptr<TableRef> ApplyTableWithBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	// First argument is the function name
	if (input.inputs.empty()) {
		throw BinderException("apply_table_with requires at least a function name");
	}

	auto &func_name_val = input.inputs[0];
	if (func_name_val.IsNull()) {
		throw BinderException("apply_table_with: function name cannot be NULL");
	}

	string func_name = StringValue::Get(func_name_val);

	// Validate function name
	if (!IsValidIdentifier(func_name)) {
		throw BinderException("apply_table_with: invalid function name '%s'", func_name);
	}

	// Get args from named parameter or second input (needed for security check)
	Value args_list;
	Value kwargs_struct;

	// Check named parameters first
	auto args_it = input.named_parameters.find("args");
	if (args_it != input.named_parameters.end()) {
		args_list = args_it->second;
	} else if (input.inputs.size() > 1) {
		args_list = input.inputs[1];
	}

	// Collect args for security validation
	vector<Value> args_for_validation;
	if (!args_list.IsNull() && args_list.type().id() == LogicalTypeId::LIST) {
		auto &list_children = ListValue::GetChildren(args_list);
		for (auto &child : list_children) {
			args_for_validation.push_back(child);
		}
	}

	// Validate against security policy (will throw if on_block = "error")
	if (!ValidateFunctionCall(context, func_name, args_for_validation)) {
		// If we get here, on_block is "null" or "default" - but table functions
		// can't return those, so we throw a specific error
		throw BinderException("apply_table_with: function '%s' is blocked by security policy", func_name);
	}

	// Check if it's a table function
	if (!TableFunctionExists(context, func_name)) {
		// Check if it's a scalar function to give a better error message
		if (GetCallableFunctionType(context, func_name) != CatalogType::INVALID) {
			throw BinderException("apply_table_with: '%s' is a scalar function. Use apply_with() instead.", func_name);
		}
		throw BinderException("apply_table_with: function '%s' does not exist", func_name);
	}

	// Get kwargs from named parameter or third input
	auto kwargs_it = input.named_parameters.find("kwargs");
	if (kwargs_it != input.named_parameters.end()) {
		kwargs_struct = kwargs_it->second;
	} else if (input.inputs.size() > 2) {
		kwargs_struct = input.inputs[2];
	}

	// Build the SQL query: SELECT * FROM func_name(arg1, arg2, ..., kwarg1 := val1, ...)
	string sql = "SELECT * FROM " + func_name + "(";
	bool first = true;

	// Add positional args from list
	if (!args_list.IsNull() && args_list.type().id() == LogicalTypeId::LIST) {
		auto &list_children = ListValue::GetChildren(args_list);
		for (auto &child : list_children) {
			if (!first) {
				sql += ", ";
			}
			sql += ValueToSQL(child);
			first = false;
		}
	}

	// Add kwargs from struct
	if (!kwargs_struct.IsNull() && kwargs_struct.type().id() == LogicalTypeId::STRUCT) {
		auto &struct_children = StructValue::GetChildren(kwargs_struct);
		auto &type = kwargs_struct.type();
		for (idx_t i = 0; i < struct_children.size(); i++) {
			if (!first) {
				sql += ", ";
			}
			auto &name = StructType::GetChildName(type, i);
			sql += name + " := " + ValueToSQL(struct_children[i]);
			first = false;
		}
	}

	sql += ")";

	// Parse and return as subquery
	return ParseSubquery(sql, context.GetParserOptions());
}

//===--------------------------------------------------------------------===//
// Security Configuration Functions
//===--------------------------------------------------------------------===//

// func_apply_set_security_mode(mode VARCHAR) -> VARCHAR
// Sets the security mode: 'none', 'blacklist', 'whitelist', 'validator'
static void SetSecurityModeScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &mode_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(
	    mode_vector, result, args.size(),
	    [&](string_t mode_str) {
		    auto &config = GetSecurityConfig(context);

		    if (config.locked) {
			    throw InvalidInputException("func_apply security settings are locked");
		    }

		    string mode = mode_str.GetString();
		    if (mode != "none" && mode != "blacklist" && mode != "whitelist" && mode != "validator") {
			    throw InvalidInputException("Invalid security mode: '%s'. Must be 'none', 'blacklist', 'whitelist', or 'validator'", mode);
		    }

		    config.mode = mode;
		    return StringVector::AddString(result, "Security mode set to: " + mode);
	    }
	);
}

// func_apply_set_blacklist(list LIST) -> VARCHAR
// Sets the blacklist of blocked functions
static void SetBlacklistScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	idx_t count = args.size();

	for (idx_t i = 0; i < count; i++) {
		auto list_val = args.data[0].GetValue(i);
		auto &config = GetSecurityConfig(context);

		if (config.locked) {
			throw InvalidInputException("func_apply security settings are locked");
		}

		config.blacklist.clear();

		if (!list_val.IsNull() && list_val.type().id() == LogicalTypeId::LIST) {
			auto &children = ListValue::GetChildren(list_val);
			for (auto &child : children) {
				if (!child.IsNull()) {
					config.blacklist.insert(StringUtil::Lower(StringValue::Get(child)));
				}
			}
		}

		result.SetValue(i, Value("Blacklist set with " + to_string(config.blacklist.size()) + " functions"));
	}
}

// func_apply_set_whitelist(list LIST) -> VARCHAR
// Sets the whitelist of allowed functions
static void SetWhitelistScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	idx_t count = args.size();

	for (idx_t i = 0; i < count; i++) {
		auto list_val = args.data[0].GetValue(i);
		auto &config = GetSecurityConfig(context);

		if (config.locked) {
			throw InvalidInputException("func_apply security settings are locked");
		}

		config.whitelist.clear();

		if (!list_val.IsNull() && list_val.type().id() == LogicalTypeId::LIST) {
			auto &children = ListValue::GetChildren(list_val);
			for (auto &child : children) {
				if (!child.IsNull()) {
					config.whitelist.insert(StringUtil::Lower(StringValue::Get(child)));
				}
			}
		}

		result.SetValue(i, Value("Whitelist set with " + to_string(config.whitelist.size()) + " functions"));
	}
}

// func_apply_set_validator(func_name VARCHAR) -> VARCHAR
// Sets the validator function name
static void SetValidatorScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &name_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name_str) {
		    auto &config = GetSecurityConfig(context);

		    if (config.locked) {
			    throw InvalidInputException("func_apply security settings are locked");
		    }

		    config.validator_func = name_str.GetString();
		    return StringVector::AddString(result, "Validator set to: " + config.validator_func);
	    }
	);
}

// func_apply_set_on_block(behavior VARCHAR) -> VARCHAR
// Sets what happens when a function is blocked: 'error', 'null', 'default'
static void SetOnBlockScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &behavior_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(
	    behavior_vector, result, args.size(),
	    [&](string_t behavior_str) {
		    auto &config = GetSecurityConfig(context);

		    if (config.locked) {
			    throw InvalidInputException("func_apply security settings are locked");
		    }

		    string behavior = behavior_str.GetString();
		    if (behavior != "error" && behavior != "null" && behavior != "default") {
			    throw InvalidInputException("Invalid on_block behavior: '%s'. Must be 'error', 'null', or 'default'", behavior);
		    }

		    config.on_block = behavior;
		    return StringVector::AddString(result, "On-block behavior set to: " + behavior);
	    }
	);
}

// func_apply_set_block_default(value ANY) -> VARCHAR
// Sets the default value to return when blocked (used with on_block='default')
static void SetBlockDefaultScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	idx_t count = args.size();

	for (idx_t i = 0; i < count; i++) {
		auto &config = GetSecurityConfig(context);

		if (config.locked) {
			throw InvalidInputException("func_apply security settings are locked");
		}

		config.block_default = args.data[0].GetValue(i);
		result.SetValue(i, Value("Block default value set"));
	}
}

// func_apply_lock_security() -> VARCHAR
// Locks security settings (one-way, cannot be unlocked)
static void LockSecurityScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	idx_t count = args.size();

	for (idx_t i = 0; i < count; i++) {
		auto &config = GetSecurityConfig(context);

		if (config.locked) {
			throw InvalidInputException("func_apply security settings are already locked");
		}

		config.locked = true;
		result.SetValue(i, Value("Security settings locked (cannot be unlocked)"));
	}
}

// func_apply_get_security_config() -> VARCHAR
// Returns the current security configuration as a JSON-like string
static void GetSecurityConfigScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	idx_t count = args.size();

	for (idx_t i = 0; i < count; i++) {
		auto &config = GetSecurityConfig(context);

		string output = "{\n";
		output += "  \"mode\": \"" + config.mode + "\",\n";
		output += "  \"on_block\": \"" + config.on_block + "\",\n";
		output += "  \"locked\": " + string(config.locked ? "true" : "false") + ",\n";
		output += "  \"validator\": \"" + config.validator_func + "\",\n";

		output += "  \"blacklist\": [";
		bool first = true;
		for (auto &func : config.blacklist) {
			if (!first) output += ", ";
			output += "\"" + func + "\"";
			first = false;
		}
		output += "],\n";

		output += "  \"whitelist\": [";
		first = true;
		for (auto &func : config.whitelist) {
			if (!first) output += ", ";
			output += "\"" + func + "\"";
			first = false;
		}
		output += "]\n";

		output += "}";

		result.SetValue(i, Value(output));
	}
}

static void LoadInternal(ExtensionLoader &loader) {
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

	// Register apply_table (table function with variadic args)
	// Uses bind_replace to generate SQL dynamically
	TableFunction apply_table_func("apply_table", {LogicalType::VARCHAR}, nullptr, nullptr);
	apply_table_func.varargs = LogicalType::ANY;
	apply_table_func.bind_replace = ApplyTableBindReplace;
	loader.RegisterFunction(apply_table_func);

	// Register apply_table_with (structured table function with args list and kwargs struct)
	// Uses bind_replace to generate SQL dynamically
	TableFunction apply_table_with_func("apply_table_with", {LogicalType::VARCHAR}, nullptr, nullptr);
	apply_table_with_func.varargs = LogicalType::ANY;
	apply_table_with_func.named_parameters["args"] = LogicalType::ANY;
	apply_table_with_func.named_parameters["kwargs"] = LogicalType::ANY;
	apply_table_with_func.bind_replace = ApplyTableWithBindReplace;
	loader.RegisterFunction(apply_table_with_func);

	//===--------------------------------------------------------------------===//
	// Security Configuration Functions
	//===--------------------------------------------------------------------===//

	// func_apply_set_security_mode(mode VARCHAR) -> VARCHAR
	auto set_security_mode_func = ScalarFunction("func_apply_set_security_mode",
	                                              {LogicalType::VARCHAR},
	                                              LogicalType::VARCHAR, SetSecurityModeScalarFun);
	loader.RegisterFunction(set_security_mode_func);

	// func_apply_set_blacklist(list LIST) -> VARCHAR
	auto set_blacklist_func = ScalarFunction("func_apply_set_blacklist",
	                                          {LogicalType::LIST(LogicalType::VARCHAR)},
	                                          LogicalType::VARCHAR, SetBlacklistScalarFun);
	loader.RegisterFunction(set_blacklist_func);

	// func_apply_set_whitelist(list LIST) -> VARCHAR
	auto set_whitelist_func = ScalarFunction("func_apply_set_whitelist",
	                                          {LogicalType::LIST(LogicalType::VARCHAR)},
	                                          LogicalType::VARCHAR, SetWhitelistScalarFun);
	loader.RegisterFunction(set_whitelist_func);

	// func_apply_set_validator(func_name VARCHAR) -> VARCHAR
	auto set_validator_func = ScalarFunction("func_apply_set_validator",
	                                          {LogicalType::VARCHAR},
	                                          LogicalType::VARCHAR, SetValidatorScalarFun);
	loader.RegisterFunction(set_validator_func);

	// func_apply_set_on_block(behavior VARCHAR) -> VARCHAR
	auto set_on_block_func = ScalarFunction("func_apply_set_on_block",
	                                         {LogicalType::VARCHAR},
	                                         LogicalType::VARCHAR, SetOnBlockScalarFun);
	loader.RegisterFunction(set_on_block_func);

	// func_apply_set_block_default(value ANY) -> VARCHAR
	auto set_block_default_func = ScalarFunction("func_apply_set_block_default",
	                                              {LogicalType::ANY},
	                                              LogicalType::VARCHAR, SetBlockDefaultScalarFun);
	loader.RegisterFunction(set_block_default_func);

	// func_apply_lock_security() -> VARCHAR
	auto lock_security_func = ScalarFunction("func_apply_lock_security",
	                                          {},
	                                          LogicalType::VARCHAR, LockSecurityScalarFun);
	loader.RegisterFunction(lock_security_func);

	// func_apply_get_security_config() -> VARCHAR
	auto get_security_config_func = ScalarFunction("func_apply_get_security_config",
	                                                {},
	                                                LogicalType::VARCHAR, GetSecurityConfigScalarFun);
	loader.RegisterFunction(get_security_config_func);
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
