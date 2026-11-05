using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Voron;

namespace Corax.Querying.Planning;

public class QueryExecution
{
    public long DrivingClauseCardinality = -1;
    public List<ClauseExecution> Executions;

    // Query can have various optimizations - these list the valid optimization clauses for this query
    public ClauseExecution SortDrivingClause;
    public ClauseExecution CompoundExactFirst;
    public ClauseExecution CompoundExactSecond;
    public ClauseExecution CompoundFieldDrivingClause;
    public ClauseExecution CompoundFieldField2Range;
    public ClauseExecution SortSeekClause;

    /// <summary>
    /// When we can tell (cheaply) how many records will be returned by this query without materializing the full results.
    /// For example, an equality / inequality query on a single field
    /// </summary>
    public long KnownExactTotal = -1;

    public CompiledPlan Plan;
    
    public long[] FieldRootPages;
    public int[] ResidualParamSlot1;
    public int[] ResidualParamSlot2;
    public int[] InRangeCounts;
    public long[] Cardinalities;
    
    // query parameters, unpacked & ready to use
    public string[] StringValues;
    public Slice[] AnalyzedSlices;
    public long[] LongValues;
    public double[] DoubleValues;
    
    public bool IsAllEntries;
    
    /// <summary>Actual strategy for this query, may differ from the <see cref="CompiledPlan.Strategy"/> when cost gate determine it isn't effective</summary>
    public ExecutionStrategy ActualStrategy = ExecutionStrategy.NotEvaluated;

    /// <summary>With `include timings()`, the reason the cost gate had for a particular execution strategy.</summary>
    public string StrategyGateReason;

    public Action PopulateScanParams;

    /// <summary>Range of values for each IN / ALL IN residual predicate.</summary>
    public ResidualInValues[] ResidualInSets;
    public SpatialFilterOp[] SpatialFilters;
    public ClauseExecution[] VectorSelects;
    
    public Func<string, Regex> RegexFactory;

    // If true, we can skip SortingMatch for the vector, it already emits results in the right order
    public bool VectorPostFilterProvidesScoreOrder;

    public bool HasSpatialOrVector => SpatialFilters is { Length: > 0 } || VectorSelects is { Length: > 0 };

    public Slice GetAnalyzedSlice(IndexSearcher indexSearcher, in FieldMetadata fieldMeta, int slot)
    {
        AnalyzedSlices ??= new Slice[StringValues.Length];
        ref Slice analyzed = ref AnalyzedSlices[slot];
        if (analyzed.HasValue == false)
            analyzed = indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, StringValues[slot]);
        return analyzed;
    }
    
    public void SetKnownClause(ClauseExecution exec, PlanTemplate t)
    {
        if (exec.IsSentinel)
            return; // sentinel clause is effectively removed, cannot be a known clause
        
        int originalIndex = exec.Clause.OriginalIndex;
        if (originalIndex == t.SortDrivingClauseIndex)
        {
            DrivingClauseCardinality = exec.Cardinality;
            SortDrivingClause = exec;
        }
        if (originalIndex == t.CompoundExact.First)
            CompoundExactFirst = exec;
        if (originalIndex == t.CompoundExact.Second)
            CompoundExactSecond = exec;
        if (originalIndex == t.CompoundFieldDrivingClause)
            CompoundFieldDrivingClause = exec;       
        if (originalIndex == t.CompoundFieldField2Range)
            CompoundFieldField2Range = exec;
        if (originalIndex == t.SortSeekHintTemplateIdx)
            SortSeekClause = exec;
    }
}

public struct ResidualInValues
{
    public int Base;
    public int Count;
    public bool HasNull;
}
