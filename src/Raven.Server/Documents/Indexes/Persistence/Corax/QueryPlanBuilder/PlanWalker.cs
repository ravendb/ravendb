using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Planning;
using Raven.Client.Exceptions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static class PlanWalker
{
    public static void RewriteClauses(ResolutionContext ctx)
    {
        var clauses = ctx.Clauses;
        BoostPropagate(ctx);
        NotCanonicalize(clauses, ctx);
        if (ctx.Metadata.IsDynamic)
            DynamicFieldNameResolve(clauses);
        GroupCollapse(clauses, ctx);
        WhenRegister(clauses, ctx);
        ThrowIfErrors(ctx);
    }

    public static void ThrowIfErrors(ResolutionContext ctx)
    {
        if (ctx.Errors.Count == 0)
            return;

        string combined = ctx.Errors.Count == 1
            ? ctx.Errors[0]
            : $"Query has {ctx.Errors.Count} validation errors:{Environment.NewLine}{string.Join(Environment.NewLine, ctx.Errors)}";
        throw new InvalidQueryException(combined);
    }


    private static void WhenRegister(List<ClauseInfo> clauses, ResolutionContext ctx)
    {
        ctx.WhenCount = 0;
        foreach (var t in clauses)
            CountWhenConditions(t, ctx);

        // A WHEN(...) guard attaches wherever the when() method appears in the query — including inside an
        // OR/AND group, where the guarded clause lives in SubClauses rather than the top-level list. Recurse
        // so the count (the ApplyFate fast-path gate) sees nested guards too.
        static void CountWhenConditions(ClauseInfo clause, ResolutionContext ctx)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (clause.WhenCondition != null)
                ctx.WhenCount++;
            foreach (var sub in clause.SubClauses ?? [])
                CountWhenConditions(sub, ctx);
        }
    }

    /// <summary>Sets the IsOrChainNotEquals to true, telling the IL emitter to materialize the complement (FillAllEntries + AndNot(positive)).</summary>
    private static void NotCanonicalize(List<ClauseInfo> clauses, ResolutionContext ctx)
    {
        foreach (var c in clauses)
        {
            NotCanonizeRecursive(c, ctx.IsOr);
        }

        static void NotCanonizeRecursive(ClauseInfo c, bool enclosingIsOr)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            c.IsOrChainNotEquals |= enclosingIsOr && c.IsNegated;
            foreach (var sub in c.SubClauses ?? [])
            {
                NotCanonizeRecursive(sub, c.ClauseType == ClauseType.OrGroup);
            }
        }
    }

    private static void DynamicFieldNameResolve(List<ClauseInfo> clauses)
    {
        foreach (var t in clauses)
        {
            DynamicFieldNameResolveRecursive(t);
        }

        void DynamicFieldNameResolveRecursive(ClauseInfo clause)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            foreach (var t in clause.SubClauses ?? [])
            {
                DynamicFieldNameResolveRecursive(t);
            }

            if (clause.FieldName == null ||
                // Spatial and Vector clauses handle their own field resolution — skip them.
                clause.ClauseType is ClauseType.Spatial or ClauseType.Vector)
                return;

            if (clause.ClauseType == ClauseType.Search)
            {
                // search() on document-id field must NOT be wrapped — id() is the document key that is not analyzed.
                if (string.Equals(clause.FieldName, Client.Constants.Documents.Indexing.Fields.DocumentIdFieldName, StringComparison.Ordinal) == false)
                {
                    clause.ResolvedFieldName = AutoIndexField.GetSearchAutoIndexFieldName(clause.FieldName);
                }
            }
            else if (clause.IsExact)
            {
                clause.ResolvedFieldName = AutoIndexField.GetExactAutoIndexFieldName(clause.FieldName);
            }
        }
    }
    private static void BoostPropagate(ResolutionContext ctx)
    {
        foreach (var pending in ctx.PendingBoosts ?? [])
        {
            foreach (var t in pending.InnerClauses)
            {
                Apply(t, pending.Factor);
            }
        }

        // a group has no Bindings of its own - carry the factor down to the leaves
        static void Apply(ClauseInfo clause, ParameterBinding factor)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (clause.ClauseType == ClauseType.Vector)
                throw new NotSupportedException("Boosting the VectorSearchMatch is not supported yet.");

            if (clause.SubClauses is { Count: > 0 } subClauses)
            {
                foreach (var sub in subClauses)
                {
                    Apply(sub, factor);
                }

                return;
            }

            clause.Bindings = [..clause.Bindings ?? [], factor];
            clause.HasBoost = true;
        }
    }

    private static void GroupCollapse(List<ClauseInfo> clauses, ResolutionContext ctx)
    {
        if (ctx.IsOr)
            return;

        for (int i = clauses.Count - 1; i >= 0; i--)
        {
            var list = clauses[i].ClauseType switch
            {
                ClauseType.Spatial => ctx.SpatialClauses ??= [],
                ClauseType.Vector => ctx.VectorClauses ??= [],
                _ => null
            };
            if(list is null) continue;
            list.Add(clauses[i]);
            clauses.RemoveAt(i);
        }
    }
}
