using System;
using System.Collections.Generic;
using Corax.Utils.Spatial;
using Sparrow.Json;

namespace Corax.Querying.Planning;

/// <summary>
/// Intermediate representation of a single WHERE predicate, between the RQL AST
/// and the PlanOp[] execution plan.
///
/// Why not reuse the RQL AST directly?
/// - RQL AST exists in the Raven.Server project, not accessible here
/// - The AST is a recursive tree (AND(AND(A,B),C)); ClauseInfo is a flat list suitable for
///   plan emission. Mixed AND/OR trees are flattened into OrGroup/AndGroup sub-lists.
/// - Field names are resolved (alias substitution, id() expansion, quoted-name handling).
/// - Parameter values are resolved from the blittable and stored as native types in the
///   plan's typed arrays (LongValues, DoubleValues, StringValues). PackedParam encodes
///   (type, index), so resolution never reparses strings.
/// - A clause type is classified into a flat enum — downstream code switches on one value
///   instead of pattern-matching AST node types and method names.
/// - Planning annotations (Cardinality, IsExact, BoostFactor, IsNegated) are attached per
///   clause for operand reordering, dispatch classification, and entry-scan eligibility.
/// </summary>
public sealed class ClauseInfo
{
    public string FieldName { get; init; }

    /// <summary>Pre-resolved dynamic-index field name variant (e.g. <c>exact(Name)</c> or <c>search(Name)</c>). </summary>
    public string ResolvedFieldName { get; set; }

    public ClauseType ClauseType { get; init; }

    public int OriginalIndex { get; init; }

    public bool IsNegated { get; set; }

    public bool IsExact { get; set; }

    public Constants.Search.Operator SearchOperator { get; init; }

    public SpatialRelation SpatialMethodType { get; init; }

    public VectorSourceKind VectorMethod { get; init; }

    /// <summary>Set for any negated clause appearing in an OR chain.
    /// Example: `WHERE Name != 'a' OR Age = 25` or `WHERE NOT exists(Tags) OR Score &gt; 10`.
    /// IL emitter builds the complement at execution time via FillAllEntries + AndNot(positive form.
    /// Boost is intentionally ignored on such clauses (matches Lucene — there is no match to score).</summary>
    public bool IsOrChainNotEquals { get; set; }

    public List<ClauseInfo> SubClauses { get; init; }

    /// <summary>Parameter bindings indexed by <see cref="BindingIndex"/> constants.
    /// If <see cref="HasBoost"/> is true, the last entry is the boost factor binding.</summary>
    public ParameterBinding[] Bindings { get; set; }

    /// <summary>True if this clause is wrapped in boost(). When set, Bindings[^1] is the
    /// boost factor binding and exec.BoostFactor is resolved from it per-execution.</summary>
    public bool HasBoost { get; set; }

    /// <summary>Optional WHEN condition delegate, called with the query's BlittableJsonReaderObject parameters; returns false to eliminate the clause.</summary>
    public Func<BlittableJsonReaderObject, bool> WhenCondition { get; set; }

    /// <summary>Shared by every CompiledPlan derived from this template and by every concurrent execution of them. Help steer range estimations over time.</summary>
    public readonly InflationEwma RangeEstimateCalibration = new();
}
