using System;
using System.Collections.Generic;

namespace Corax.Querying.Planning;

[Flags]
public enum PlanOptimizationFlags : byte
{
    None = 0,
    CompoundExactCandidate = 1,
    DirectScanCandidate = 2,
}

/// <summary>Immutable structural template built on the first execution of a query text.  Cached on PerQueryPlans.Template. On cache hit, clauses are cloned and their
/// per-execution fields overwritten by PopulateParameters.</summary>
public sealed class PlanTemplate
{
    public List<ClauseInfo> Clauses;
    public bool IsOr;              

    /// <summary>Spatial clauses separated from the main filter chain (AND queries only).</summary>
    public List<ClauseInfo> SpatialClauses;
    /// <summary>Vector clauses separated from the main filter chain (AND queries only).</summary>
    public List<ClauseInfo> VectorClauses;

    public PlanOptimizationFlags OptimizationFlags;

    /// <summary>Pre-identified direct scan driving clause. (range/eq on the primary ORDER BY field, non-negated, non-boosted). </summary>
    public int SortDrivingClauseIndex = -1;

    /// <summary>Pre-identified compound-exact-match clause pair (template-position indices).</summary>
    public (int First, int Second) CompoundExact = (-1, -1);
    
    /// <summary>True when compound field order is (A, B); false when (B, A).</summary>
    public bool CompoundExactAFirst;
    /// <summary>Pre-built <c>compound({firstField},{secondField})</c> tree name for the compound-exact match.</summary>
    public string CompoundExactName;

    /// <summary>Pre-identified compound-field-match (WHERE Equals + ORDER BY) driving clause  index.</summary>
    public int CompoundFieldDrivingClause = -1;

    public string CompoundFieldSortName;
    public string CompoundFieldName;

    /// <summary>
    /// Consider a query below with compound(Category, Price):
    /// 
    ///     from Products where Category = 'Electronics' and Price > 100 order by Price
    ///
    /// We can start the scan of the compound field from gt ('Electronics', 100), with
    /// this approach.
    /// </summary>
    public int CompoundFieldField2Range = -1;

   /// <summary>Count of WHEN() clauses</summary>
    public int WhenCount;

    /// <summary>Deduplicated, ordered list of query parameter names referenced by this template's clause bindings (<see cref="BindingSource.QueryParameter"/> only).
    /// Used to compute the TypeSignature cache-key component cheaply at execution time by classifying each parameter's runtime blittable type.</summary>
    public string[] ParameterSlots = [];

    /// <summary>Template-position index of the clause that supplies the seek value for <c>TrySetSortSeekHint</c>.</summary>
    public int SortSeekHintTemplateIdx = -1;

    /// <summary>For the BETWEEN seek hint: true when descending order (read Param2 = upper bound), used with <see cref="SortSeekHintTemplateIdx"/>.</summary>
    public bool SortSeekUseParam2;

    /// <summary>Number of value-bearing bindings (literal / query-parameter / deferred-method) for this query.</summary>
    public int ValueOrdinalCount;

    /// <summary>Pre-computed sort-metadata template. Everything we need to handle sorting properly.</summary>
    public SortMetadataTemplate SortMetadataTemplate;
}
