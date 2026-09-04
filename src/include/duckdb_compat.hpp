#pragma once

#include "duckdb.hpp"
#include <type_traits>

//===--------------------------------------------------------------------===//
// duckdb_compat.hpp -- source compatibility with BOTH DuckDB lines
//===--------------------------------------------------------------------===//
//
// func_apply SHIPS against the pinned stable DuckDB (v1.5.x). This header is
// not a migration: it lets the same sources also compile against DuckDB `main`
// (the v2.0 line), which duckdb/community-extensions builds every release PR
// against via `build_next.yml`'s `test_against_latest` job. That job has no
// per-extension opt-out, so an extension that does not build there shows red on
// every release PR.
//
// FEATURE DETECTION, NOT VERSION NUMBERS. A version macro says *when* something
// changed; a probe says whether it changed *here*, which keeps working when a
// change is backported, reverted, or lands on an unexpected branch.
//
// PROBE POLARITY MATTERS. Always probe for the API that exists ONLY on v2.0,
// never for the one being replaced. Several v1.5 APIs were deprecated rather
// than deleted on main, so a probe aimed at the *old* API is true on BOTH
// versions and the shim silently never takes the new branch.
//
// C++ STANDARD. duckdb v1.5.x's own TUs are built at -std=c++11, and this
// extension deliberately follows (see the comment in CMakeLists.txt: forcing
// C++17 on our TUs but not on libduckdb's makes static-const data members in
// duckdb's headers acquire implicit inline linkage in one set and not the
// other, producing multiple-definition link errors). So nothing here may
// require C++17: `if constexpr` is out, and member dispatch is written as tag
// dispatch, which has the same "only the selected overload is instantiated"
// property at both standards.

// Probe 1: the vector-buffer header reshuffle (duckdb/duckdb#22377). Present on
// main, absent on v1.5.x. Used for the changes that landed with the v2.0 API
// rework: the scalar bind-callback signature and the scalar-function /
// expression accessors.
#if __has_include("duckdb/common/vector/list_vector.hpp")
#define DUCKDB_HAS_NEW_VECTOR_HEADERS 1
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#endif

// Probe 2: duckdb::Identifier, which replaced std::string as the name type in
// child_list_t (STRUCT field names), named_parameter_map_t keys, catalog lookup
// parameters, BaseExpression::alias and the FunctionExpression constructor.
// Identifier compares case-insensitively, which is why it exists. Its
// conversions are deliberately asymmetric:
//     Identifier(const char *)            -- IMPLICIT (literals are identifiers by intent)
//     explicit Identifier(const string &) -- EXPLICIT (promoting a runtime string is deliberate)
// so only the places a *runtime* string crosses the boundary need a helper, and
// string literals such as DEFAULT_SCHEMA keep compiling untouched.
//
// Probed separately from probe 1 on purpose: tying two independent changes to
// one macro silently picks the wrong branch if they ever land in different
// releases.
#if __has_include("duckdb/common/identifier.hpp")
#define DUCKDB_HAS_IDENTIFIER 1
#include "duckdb/common/identifier.hpp"
#endif

#if __has_include("duckdb/planner/expression/bound_function_expression.hpp")
#include "duckdb/planner/expression/bound_function_expression.hpp"
#endif
#if __has_include("duckdb/function/scalar_function.hpp")
#include "duckdb/function/scalar_function.hpp"
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Identifier boundary helpers
//===--------------------------------------------------------------------===//

#ifdef DUCKDB_HAS_IDENTIFIER
using CompatName = Identifier;
inline const string &CompatNameStr(const Identifier &id) {
	return id.GetIdentifierName();
}
inline const string &CompatNameStr(const string &name) {
	return name;
}
inline Identifier CompatMakeName(string name) {
	return Identifier(std::move(name));
}
#else
using CompatName = string;
inline const string &CompatNameStr(const string &name) {
	return name;
}
inline string CompatMakeName(string name) {
	return name;
}
#endif

//===--------------------------------------------------------------------===//
// Scalar bind-callback signature
//===--------------------------------------------------------------------===//
//
// v1.5: unique_ptr<FunctionData> (*)(ClientContext &, ScalarFunction &,
//                                    vector<unique_ptr<Expression>> &)
// v2.0: unique_ptr<FunctionData> (*)(BindScalarFunctionInput &)
//
// Declare bind callbacks as `Bind(DUCKDB_SCALAR_BIND_PARAMS)` and open the body
// with the new-path unpacking, so the rest of the body is identical on both:
//
//   static unique_ptr<FunctionData> MyBind(DUCKDB_SCALAR_BIND_PARAMS) {
//   #ifdef DUCKDB_HAS_NEW_VECTOR_HEADERS
//       auto &context = bind_input.GetClientContext();
//       auto &arguments = bind_input.GetArguments();
//       auto &bound_function = bind_input.GetBoundFunction();
//   #endif
//       ...
//   }
//
// Note that on v2.0 `bound_function` is a BoundScalarFunction rather than a
// ScalarFunction, which is why CompatBindSetReturnType below is a template.

} // namespace duckdb

#ifdef DUCKDB_HAS_NEW_VECTOR_HEADERS
#define DUCKDB_SCALAR_BIND_PARAMS  duckdb::BindScalarFunctionInput &bind_input
#define DUCKDB_SCALAR_BIND_CONTEXT bind_input.GetClientContext()
#define DUCKDB_SCALAR_BIND_ARGS    bind_input.GetArguments()
#else
#define DUCKDB_SCALAR_BIND_PARAMS                                                                                      \
	duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function,                                            \
	    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments
#define DUCKDB_SCALAR_BIND_CONTEXT context
#define DUCKDB_SCALAR_BIND_ARGS    arguments
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Public fields that became private/protected, and their accessors
//===--------------------------------------------------------------------===//
//
// This is the largest change class by error count and the one a source grep
// cannot find -- there is no distinctive token to search for.
//
//   ScalarFunction::varargs            -> Get/SetVarArgs()
//   ScalarFunction::null_handling      -> Get/SetNullHandling()
//   ScalarFunction::return_type        -> Get/SetReturnType()
//   Expression::return_type            (protected) -> GetReturnType()
//   BaseExpression::alias              (protected, and now an Identifier) -> GetAlias()
//   BoundFunctionExpression::bind_info (private)   -> BindInfo()/BindInfoMutable()
//   BoundFunctionExpression::children  (private)   -> GetChildren()/GetChildrenMutable()

#ifdef DUCKDB_HAS_NEW_VECTOR_HEADERS

inline const LogicalType &CompatExprReturnType(const Expression &e) {
	return e.GetReturnType();
}
inline vector<unique_ptr<Expression>> &CompatBoundChildren(BoundFunctionExpression &e) {
	return e.GetChildrenMutable();
}
inline const vector<unique_ptr<Expression>> &CompatBoundChildren(const BoundFunctionExpression &e) {
	return e.GetChildren();
}
inline unique_ptr<FunctionData> &CompatBoundBindInfo(BoundFunctionExpression &e) {
	return e.BindInfoMutable();
}
inline const unique_ptr<FunctionData> &CompatBoundBindInfo(const BoundFunctionExpression &e) {
	return e.BindInfo();
}
inline void CompatSetScalarReturnType(ScalarFunction &f, LogicalType t) {
	f.SetReturnType(std::move(t));
}
inline void CompatSetScalarNullHandling(ScalarFunction &f, FunctionNullHandling h) {
	f.SetNullHandling(h);
}
inline void CompatSetScalarVarArgs(ScalarFunction &f, LogicalType v) {
	f.SetVarArgs(std::move(v));
}

#else

inline const LogicalType &CompatExprReturnType(const Expression &e) {
	return e.return_type;
}
inline vector<unique_ptr<Expression>> &CompatBoundChildren(BoundFunctionExpression &e) {
	return e.children;
}
inline const vector<unique_ptr<Expression>> &CompatBoundChildren(const BoundFunctionExpression &e) {
	return e.children;
}
inline unique_ptr<FunctionData> &CompatBoundBindInfo(BoundFunctionExpression &e) {
	return e.bind_info;
}
inline const unique_ptr<FunctionData> &CompatBoundBindInfo(const BoundFunctionExpression &e) {
	return e.bind_info;
}
inline void CompatSetScalarReturnType(ScalarFunction &f, LogicalType t) {
	f.return_type = std::move(t);
}
inline void CompatSetScalarNullHandling(ScalarFunction &f, FunctionNullHandling h) {
	f.null_handling = h;
}
inline void CompatSetScalarVarArgs(ScalarFunction &f, LogicalType v) {
	f.varargs = std::move(v);
}

#endif

//! BaseExpression::GetAlias() exists on both lines; only its return type moved
//! (string -> Identifier), so this needs no #ifdef, just the name helper.
inline const string &CompatExprAlias(const BaseExpression &e) {
	return CompatNameStr(e.GetAlias());
}

//! Set the return type from inside a bind callback. Templated because the type
//! of `bound_function` differs by version: ScalarFunction on v1.5,
//! BoundScalarFunction on v2.0. Both spell the setter SetReturnType(), so no
//! probe is needed -- only late binding of the type.
template <class F>
inline void CompatBindSetReturnType(F &f, LogicalType t) {
	f.SetReturnType(std::move(t));
}

//===--------------------------------------------------------------------===//
// FunctionSet<T>::functions yields shared_ptr<const T>
//===--------------------------------------------------------------------===//
//
// v1.5: vector<T>                    -- element is a T, mutable
// v2.0: vector<shared_ptr<const T>>  -- overloads are immutable and shared with
//                                       every expression bound from them
//
// For read-only introspection (which is all func_apply does), one dereference
// is the whole difference. Overload resolution picks the shared_ptr form by
// partial ordering when the element is a shared_ptr and the identity form
// otherwise; both spellings name valid types on both versions, so this needs no
// probe.
//
// There is deliberately no mutable variant. On v2.0 a set's overloads must be
// finished BEFORE they are added, not configured in place afterwards -- a loop
// like `for (auto &f : set.functions) { f.SetStability(...); }` no longer
// compiles, and the fix is to reorder, not to shim.
template <class T>
inline const T &CompatFunctionRef(const T &function) {
	return function;
}
template <class T>
inline const T &CompatFunctionRef(const shared_ptr<const T> &function) {
	return *function;
}

//===--------------------------------------------------------------------===//
// FunctionProperties::capture_argument_aliases
//===--------------------------------------------------------------------===//
//
// v1.5 always recorded a named argument's alias (`f(x := 1)`) on the bound child
// expression, so a bind callback could recover the parameter name with
// GetAlias(). v2.0 made that opt-in and defaults it to FALSE: without the flag
// the aliases come back EMPTY, a function that derives named parameters from
// them silently binds every argument as unnamed, and the failure surfaces at
// RUNTIME with a perfectly green build. Nothing to grep for, no compile error.
//
// Probed on the member, which exists only on v2.0. A no-op on v1.5, where the
// behaviour is already unconditional.
template <class T, class = void>
struct CompatHasCaptureArgumentAliases : std::false_type {};
template <class T>
struct CompatHasCaptureArgumentAliases<T, decltype(void(std::declval<T &>().SetCaptureArgumentAliases(true)))>
    : std::true_type {};

template <class F>
inline void CompatSetCaptureArgumentAliasesImpl(F &f, std::true_type) {
	f.SetCaptureArgumentAliases(true);
}
template <class F>
inline void CompatSetCaptureArgumentAliasesImpl(F &, std::false_type) {
}
//! Keep argument aliases visible to the function's own bind callback.
template <class F>
inline void CompatSetCaptureArgumentAliases(F &f) {
	CompatSetCaptureArgumentAliasesImpl(f, CompatHasCaptureArgumentAliases<F>());
}

//===--------------------------------------------------------------------===//
// LogicalType::SetAlias -> WithAlias
//===--------------------------------------------------------------------===//
//
// v2.0's WithAlias returns a copy rather than mutating a type whose type-info
// may be shared; SetAlias is REMOVED, not deprecated. Probed on the member
// (v2.0-only) rather than on either macro above, since it is an independent
// change.
template <class T, class = void>
struct CompatHasWithAlias : std::false_type {};
template <class T>
struct CompatHasWithAlias<T, decltype(void(std::declval<const T &>().WithAlias(string())))> : std::true_type {};

template <class TYPE>
inline LogicalType CompatWithAliasImpl(TYPE type, string alias, std::true_type) {
	return type.WithAlias(std::move(alias));
}
template <class TYPE>
inline LogicalType CompatWithAliasImpl(TYPE type, string alias, std::false_type) {
	type.SetAlias(std::move(alias));
	return type;
}
//! Entry point is CONCRETE, not `template <class TYPE = LogicalType>`. A default
//! template argument is inert -- deduction wins -- and LogicalType::VARCHAR is a
//! static constexpr LogicalTypeId, not a LogicalType, so the templated form
//! deduced TYPE = LogicalTypeId and hard-errored on the PINNED build with
//! "request for member 'SetAlias' in 'type', which is of non-class type
//! 'duckdb::LogicalTypeId'". Taking LogicalType by value forces the conversion
//! at the call site instead.
inline LogicalType CompatWithAlias(LogicalType type, string alias) {
	return CompatWithAliasImpl(std::move(type), std::move(alias), CompatHasWithAlias<LogicalType>());
}

//===--------------------------------------------------------------------===//
// FlatVector::GetData<T> is read-only on v2.0; writes need GetDataMutable<T>
//===--------------------------------------------------------------------===//
//
// Probed on GetDataMutable, which exists only on v2.0 -- probing GetData would
// be true on both versions and would always take the wrong branch.
template <class T, class = void>
struct CompatHasFlatGetDataMutable : std::false_type {};
template <class T>
struct CompatHasFlatGetDataMutable<T, decltype(void(T::template GetDataMutable<bool>(std::declval<Vector &>())))>
    : std::true_type {};

template <class VALUE, class FV>
inline VALUE *CompatFlatDataMutableImpl(Vector &vec, std::true_type) {
	return FV::template GetDataMutable<VALUE>(vec);
}
template <class VALUE, class FV>
inline VALUE *CompatFlatDataMutableImpl(Vector &vec, std::false_type) {
	return FV::template GetData<VALUE>(vec);
}
template <class VALUE, class FV = FlatVector>
inline VALUE *CompatFlatDataMutable(Vector &vec) {
	return CompatFlatDataMutableImpl<VALUE, FV>(vec, CompatHasFlatGetDataMutable<FV>());
}

//===--------------------------------------------------------------------===//
// UnaryExecutor/BinaryExecutor::ExecuteWithNulls -- removed on v2.0
//===--------------------------------------------------------------------===//
//
// func_apply deliberately carries NO shim for this, because it has no callsite
// that needs one: every executor call here is already a plain Execute. The note
// stays because the obvious replacement is wrong and fails silently.
//
// ExecuteWithNulls copied the input validity into the result mask and then
// SKIPPED the lambda entirely for null rows. So a lambda opening with
// `if (!mask.RowIsValid(idx)) return X;` never ran that branch -- such a
// function has always returned NULL for null input. The ValidityMask &
// parameter is there so a function can ADD nulls on valid rows, not so it can
// observe them.
//
// When that branch is dead, the exact equivalent is a plain `Execute`: it
// propagates nulls identically, exists on both versions, and needs no shim. A
// hand-rolled UnifiedVectorFormat loop is NOT equivalent -- it writes a value
// where the original left the row NULL and never touches the result validity
// mask, silently turning NULL into that value.
//
// Only a lambda that genuinely PRODUCES nulls on valid rows (one that calls
// mask.SetInvalid(idx)) needs v2.0's optional<RESULT_TYPE> form; duckdb_yaml's
// CompatUnaryExecuteWithNulls / CompatBinaryExecuteWithNulls are the reference
// implementation should that ever become true here.
//
// Test any such change with a NULL inside a NON-CONSTANT vector --
// `SELECT v, f(v) FROM (VALUES ('x'), (NULL)) t(v)`. `SELECT f(NULL)` returns
// NULL on every version because constant folding propagates the null above the
// function before it runs, which hides the bug completely.

} // namespace duckdb
