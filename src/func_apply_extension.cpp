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
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto entry = catalog.GetEntry(context, type, DEFAULT_SCHEMA, func_name, OnEntryNotFound::RETURN_NULL);

	if (entry) {
		// CRITICAL: Verify the entry type matches what we asked for!
		// The catalog returns any entry with the name, regardless of type.
		if (entry->type != type) {
			return false;
		}
	}

	return entry != nullptr;
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

// Execute a function by name with given argument values
// Uses expression-based execution to avoid query planner deadlock
// Handles both scalar functions and macros
//
// NOTE: This is called at runtime for each row. The function type was already
// determined at bind time in BindApply, but we re-check here because the function
// name could be dynamic (coming from a column value).
static Value ExecuteFunction(ClientContext &context, const string &func_name, const vector<Value> &args) {
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

	// Check if it's a table function
	if (!TableFunctionExists(context, func_name)) {
		// Check if it's a scalar function to give a better error message
		if (GetCallableFunctionType(context, func_name) != CatalogType::INVALID) {
			throw BinderException("apply_table_with: '%s' is a scalar function. Use apply_with() instead.", func_name);
		}
		throw BinderException("apply_table_with: function '%s' does not exist", func_name);
	}

	// Get args from named parameter or second input
	Value args_list;
	Value kwargs_struct;

	// Check named parameters first
	auto args_it = input.named_parameters.find("args");
	if (args_it != input.named_parameters.end()) {
		args_list = args_it->second;
	} else if (input.inputs.size() > 1) {
		args_list = input.inputs[1];
	}

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
