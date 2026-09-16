using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static void PopulateHighlightingTerms(QueryExecution exec, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        foreach (var cur in exec.Executions)
        {
            PopulateHighlightingForClause(cur, highlightingTerms, metadata, exec);
        }
    }

    private static void PopulateHighlightingForClause(ClauseExecution exec, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata, QueryExecution queryExec)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (exec.ClauseType)
        {
            case ClauseType.OrGroup or ClauseType.AndGroup:
            {
                foreach (var subExecution in exec.SubExecutions)
                {
                    PopulateHighlightingForClause(subExecution, highlightingTerms, metadata, queryExec);
                }
                return;
            }
        }
        
        ClauseInfo clause = exec.Clause;
        if (clause.FieldName == null)
            return; // can happen if we have a method, etc

        if (clause.ClauseType is ClauseType.Equals or ClauseType.NotEquals)
        {
            if (exec.PackedParamValue.IsNone || // ignore null values that are == / != 
                (exec.PackedParamValue.ValueType == PackedParam.TypeString && queryExec.StringValues[exec.PackedParamValue.Param1] == null))
                return;
        }

        if (highlightingTerms.TryGetValue(clause.FieldName, out var existingTerm))
        {
            existingTerm.Values ??= GetHighlightingValues(clause, exec, queryExec);
            return;
        }

        var term = new CoraxHighlightingTermIndex
        {
            FieldName = clause.FieldName,
            Values = GetHighlightingValues(clause, exec, queryExec)
        };
        if (metadata.IsDynamic)
        {
            if (clause.ClauseType == ClauseType.Search)
                term.DynamicFieldName = AutoIndexField.GetSearchAutoIndexFieldName(clause.FieldName);
            else if (clause.IsExact)
                term.DynamicFieldName = AutoIndexField.GetExactAutoIndexFieldName(clause.FieldName);
        }
        
        highlightingTerms[clause.FieldName] = term;

        if (term.DynamicFieldName != null) // For dynamic indexes, also add the dynamic field name variant
            highlightingTerms[term.DynamicFieldName] = term;
    }

    private static object GetHighlightingValues(ClauseInfo clause, ClauseExecution exec, QueryExecution queryExec)
    {
        if (clause.ClauseType == ClauseType.Between)
        {
            return new List<string>
            {
                FormatValueFromPlan(exec.PackedParamValue, queryExec, exec.PackedParamValue.Param1), 
                FormatValueFromPlan(exec.PackedParamValue, queryExec, exec.PackedParamValue.Param2)
            };
        }

        if (clause.ClauseType is ClauseType.In or ClauseType.AllIn && (exec.InTermCount > 0 || exec.HasNullTerm))
        {
            var terms = new List<string>(exec.InTermCount + (exec.HasNullTerm ? 1 : 0));
            for (int t = 0; t < exec.InTermCount; t++)
            {
                PackedParam packed = exec.PackedParamValue.WithTermOffset(t);
                terms.Add(FormatValueFromPlan(packed, queryExec, packed.Param1));
            }

            if (exec.HasNullTerm)
                terms.Add(null);
            return terms;
        }

        return FormatValueFromPlan(exec.PackedParamValue, queryExec, exec.PackedParamValue.Param1);
    }
}
