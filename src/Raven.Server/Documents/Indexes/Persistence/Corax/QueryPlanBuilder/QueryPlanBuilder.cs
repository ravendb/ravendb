using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using Corax.Querying.Planning;
using Corax.Utils.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Voron;
using Constants = Corax.Constants;
using ClientConstants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static ParamValueType ToParamValueType(ValueTokenType t)
    {
        return t switch
        {
            ValueTokenType.Long => ParamValueType.Long,
            ValueTokenType.Double => ParamValueType.Double,
            ValueTokenType.Parameter => ParamValueType.Parameter,
            _ => ParamValueType.String
        };
    }

    private static ValueTokenType ToValueTokenType(ParamValueType t)
    {
        return t switch
        {
            ParamValueType.Long => ValueTokenType.Long,
            ParamValueType.Double => ValueTokenType.Double,
            ParamValueType.Parameter => ValueTokenType.Parameter,
            _ => ValueTokenType.String
        };
    }

    private static SpatialRelation ToSpatialOp(MethodType t)
    {
        return t switch
        {
            MethodType.Spatial_Within => SpatialRelation.Within,
            MethodType.Spatial_Contains => SpatialRelation.Contains,
            MethodType.Spatial_Disjoint => SpatialRelation.Disjoint,
            MethodType.Spatial_Intersects => SpatialRelation.Intersects,
            _ => SpatialRelation.Within
        };
    }

    private static PlanTemplate ParseTemplate(PlanParameters p)
    {
        QueryExpression where = p.Metadata.Query.Where;
        if (where == null)
        {
            // A bare sort with no WHERE clause is a full index scan: a direct-scan candidate.
            bool hasBareSort = p.Metadata.OrderBy is { Length: > 0 } && p.Metadata.OrderBy[0].Name?.Value is not null;
            return new PlanTemplate
            {
                Clauses = [],
                OptimizationFlags = hasBareSort ? PlanOptimizationFlags.DirectScanCandidate : PlanOptimizationFlags.None,
            };
        }

        ResolutionContext walkerCtx = new(p) { Clauses = [] };
        BooleanOp rootOp = ParseExpression(where, walkerCtx);
        PlanWalker.ThrowIfErrors(walkerCtx);

        if (rootOp == BooleanOp.True || walkerCtx.Clauses.Count == 0)
            return new PlanTemplate { Clauses = [], ValueOrdinalCount = walkerCtx.SlotBindings.Count };

        Debug.Assert(rootOp != BooleanOp.False, "No RQL expression currently reduces to BooleanOp.False at template time. ");

        walkerCtx.IsOr = rootOp == BooleanOp.Or;

        PlanWalker.RewriteClauses(walkerCtx);

        if (walkerCtx.Clauses.Count == 0 && (walkerCtx.SpatialClauses ?? walkerCtx.VectorClauses) is not null)
        {
            return new PlanTemplate
            {
                Clauses = [],
                SpatialClauses = walkerCtx.SpatialClauses,
                VectorClauses = walkerCtx.VectorClauses,
                ValueOrdinalCount = walkerCtx.SlotBindings.ToArray().Length,
            };
        }

        // Partial sort elision: drop ORDER BY keys pinned to a constant by a top-level equality and single-valued.
        OrderByField[] orderBy = ComputeEffectiveOrderBy(p.Metadata.OrderBy, walkerCtx.Clauses, walkerCtx.IsOr, p.IndexSearcher);
        string orderByPrimaryField = orderBy is { Length: > 0 }
            ? orderBy[0].Name?.Value
            : null;
        bool orderByPrimaryAscending = orderBy is [{ Ascending: true }, ..];

        PlanOptimizationFlags optFlags = ComputeTemplateOptimizations(walkerCtx, p, orderBy, orderByPrimaryField, orderByPrimaryAscending,
            out int sortDrivingIdx, out int sortSeekHintIdx, out bool sortSeekUseParam2);
        
        Dictionary<string, int> slots = [];
        AssignParameterSlots(walkerCtx.Clauses, slots);
        AssignParameterSlots(walkerCtx.SpatialClauses, slots);
        AssignParameterSlots(walkerCtx.VectorClauses, slots);

        string[] parameterSlots = slots.Keys.ToArray();
        p.Metadata.CachedSlotBindings = walkerCtx.SlotBindings.ToArray();

        return new PlanTemplate
        {
            Clauses = walkerCtx.Clauses,
            IsOr = walkerCtx.IsOr,
            SpatialClauses = walkerCtx.SpatialClauses,
            VectorClauses = walkerCtx.VectorClauses,
            WhenCount = walkerCtx.WhenCount,
            OptimizationFlags = optFlags,
            SortDrivingClauseIndex = sortDrivingIdx,
            CompoundExact =  walkerCtx.CompoundExact,
            CompoundExactAFirst = walkerCtx.CompoundExactAFirst,
            CompoundExactName = walkerCtx.CompoundExactName,
            CompoundFieldDrivingClause = walkerCtx.CompoundFieldDrivingClause,
            CompoundFieldSortName = walkerCtx.CompoundFieldSortName,
            CompoundFieldName = walkerCtx.CompoundFieldName,
            CompoundFieldField2Range = walkerCtx.CompoundFieldField2Range,
            ParameterSlots = parameterSlots,
            SortSeekHintTemplateIdx = sortSeekHintIdx,
            SortSeekUseParam2 = sortSeekUseParam2,
            ValueOrdinalCount = walkerCtx.SlotBindings.Count,
        };
    }

    private static ParameterBinding[] ExtractSlotBindings(PlanParameters p)
    {
        QueryExpression where = p.Metadata.Query.Where;
        if (where == null)
            return [];

        ResolutionContext ctx = new(p) { Clauses = [] };
        ParseExpression(where, ctx);
        PlanWalker.ThrowIfErrors(ctx);
        return ctx.SlotBindings.ToArray();
    }

    private static void AssignParameterSlots(List<ClauseInfo> clauses, Dictionary<string, int> slots)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        foreach (ClauseInfo clause in clauses ?? [])
        {
            foreach (ParameterBinding binding in clause.Bindings ?? [])
            {
                if (binding is not { Source: BindingSource.QueryParameter, ParameterName: not null }) 
                    continue;
                if (slots.TryGetValue(binding.ParameterName, out int slot) == false)
                    slots.Add(binding.ParameterName, slot = slots.Count);
                binding.ParameterSlot = slot;
            }

            AssignParameterSlots(clause.SubClauses, slots); 
        }
    }

    [SkipLocalsInit]
    private static PlanOptimizationFlags ComputeTemplateOptimizations(
        ResolutionContext walkerCtx, PlanParameters p, OrderByField[] orderBy, string orderByPrimaryField, bool orderByPrimaryAscending,
        out int sortDrivingIdx, out int sortSeekHintIdx, out bool sortSeekUseParam2)
    {
        sortDrivingIdx = -1;
        sortSeekHintIdx = -1;
        sortSeekUseParam2 = false;
        PlanOptimizationFlags flags = PlanOptimizationFlags.None;
        List<ClauseInfo> clauses = walkerCtx.Clauses;

        // Collect non-negated, non-boosted Equals clause indices for compound lookups.
        const int maxStackAllocSize = 128;
        Span<int> eqBuf = clauses.Count <= maxStackAllocSize ? stackalloc int[maxStackAllocSize] : new int[clauses.Count];
        int eqCount = 0;

        for (int i = 0; i < clauses.Count; i++)
        {
            ClauseInfo c = clauses[i];

            if (HasBoostRecursive(c)) // Any boost anywhere rules out FieldSortedScan and CompoundSortedScan (no scoring stage).
                return PlanOptimizationFlags.None;

            if (c.IsNegated)
                continue;

            if (c.ClauseType == ClauseType.Equals)
            {
                eqBuf[eqCount++] = i;
            }

            if (orderByPrimaryField is null || c.FieldName != orderByPrimaryField)
                continue;

            if (c.ClauseType is not (
                ClauseType.Equals or ClauseType.GreaterThan or
                ClauseType.GreaterThanOrEqual or ClauseType.LessThan or
                ClauseType.LessThanOrEqual or ClauseType.Between)
               )
            {
                continue;
            }

            flags |= PlanOptimizationFlags.DirectScanCandidate;
            if (sortDrivingIdx == -1 && c.WhenCondition is null)
                sortDrivingIdx = i;

            if (sortSeekHintIdx != -1)
                continue;

            switch (c.ClauseType, orderByPrimaryAscending)
            {
                case (ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual, true):
                case (ClauseType.LessThan or ClauseType.LessThanOrEqual, false):
                    sortSeekHintIdx = i;
                    sortSeekUseParam2 = false;
                    break;
                case (ClauseType.Between, _):
                    sortSeekHintIdx = i;
                    sortSeekUseParam2 = !orderByPrimaryAscending;
                    break;
            }
        }

        if (walkerCtx.IsOr || // cannot optimize: `where a OR b`
            p.Index is not { HasCompoundFields: true } ||
            eqCount is 0)
        {
            return flags;
        }

        // Compound-exact pair: two Equals clauses whose fields form a compound field.
        if (eqCount >= 2) TryFindCompoundFieldEqualMatches(eqBuf);

        // CompoundKeyLookup collapses two WHERE A = $x AND B = $y into compound($x, $y) lookup
        if (walkerCtx.CompoundExact.First >= 0 &&
            clauses.Count == 2 &&
            clauses[walkerCtx.CompoundExact.First].WhenCondition is null &&
            clauses[walkerCtx.CompoundExact.Second].WhenCondition is null)
        {
            flags |= PlanOptimizationFlags.CompoundExactCandidate;
        }

        // Compound-field candidate: Equals clause + a single ORDER BY field forming a compound field. 
        if (orderBy is [{ Name.Value: { } sf }]) 
        {
            // from Movies where Category = 'Action' order by Category, Year
            //  we elided the Category (in ComputeEffectiveOrderBy), so we have orderBy [Year]
            //  then we search for a compound field with (Category, Year), to use the direct scan optimization
            for (int e = 0; e < eqCount; e++) // search for matching compound field
            {
                string ef = clauses[eqBuf[e]].ResolvedFieldName ?? clauses[eqBuf[e]].FieldName;
                if (p.Index.HasCompoundField(p.Allocator, ef, sf) == false)
                    continue;
                walkerCtx.CompoundFieldDrivingClause = eqBuf[e];
                walkerCtx.CompoundFieldSortName = sf;
                flags |= PlanOptimizationFlags.DirectScanCandidate;
                break;
            }
        }

        // Optional field2 range narrowing clause: a GT/GTE/LT/LTE/Between on the compound sort field.
        // For example: WHERE Age > $x ORDER BY Age - will seek to the right area in the index, then run from there
        if (walkerCtx.CompoundFieldDrivingClause != -1)
        {
            walkerCtx.CompoundFieldName = $"compound({clauses[walkerCtx.CompoundFieldDrivingClause].FieldName},{walkerCtx.CompoundFieldSortName})";

            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == walkerCtx.CompoundFieldDrivingClause) continue;
                ClauseInfo c = clauses[i];
                if (c.FieldName != walkerCtx.CompoundFieldSortName) continue;
                if (c.ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                    or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between)
                {
                    walkerCtx.CompoundFieldField2Range = i;
                    break;
                }
            }
        }

        return flags;

        static bool HasBoostRecursive(ClauseInfo c)
        {
            if (c.HasBoost)
                return true;
            RuntimeHelpers.EnsureSufficientExecutionStack();
            foreach (ClauseInfo t in c.SubClauses ?? [])
            {
                if (HasBoostRecursive(t))
                    return true;
            }
            return false;
        }

        void TryFindCompoundFieldEqualMatches(Span<int> eqBuf)
        {
            for (int a = 0; a < eqCount; a++)
            {
                ClauseInfo c1 = clauses[eqBuf[a]];
                string f1 = c1.ResolvedFieldName ?? c1.FieldName;
                using var _ = Slice.From(p.Allocator, f1, out Slice s1);
                for (int b = a + 1; b < eqCount; b++)
                {
                    ClauseInfo c2 = clauses[eqBuf[b]];
                    string f2 = c2.ResolvedFieldName ?? c2.FieldName;
                    using var __ = Slice.From(p.Allocator, f2, out Slice s2);
                    if (p.Index.HasCompoundField(s1, s2))
                    {
                        walkerCtx.CompoundExact = (eqBuf[a], eqBuf[b]);
                        walkerCtx.CompoundExactAFirst = true;
                        walkerCtx.CompoundExactName = $"compound({f1},{f2})";
                        return;
                    }

                    if (p.Index.HasCompoundField(s2, s1))
                    {
                        walkerCtx.CompoundExact = (eqBuf[a], eqBuf[b]);
                        walkerCtx.CompoundExactAFirst = false;
                        walkerCtx.CompoundExactName = $"compound({f2},{f1})";
                        return;
                    }
                }
            }
        }
    }

    private static BooleanOp ParseExpression(QueryExpression expr, ResolutionContext walkerCtx)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (expr)
        {
            case BinaryExpression be:
                return ParseBinaryExpression(be, walkerCtx);

            case BetweenExpression between:
                ParseBetween(between, walkerCtx);
                return BooleanOp.Leaf;

            case InExpression inExpr:
                ParseIn(inExpr, walkerCtx);
                return BooleanOp.Leaf;

            case MethodExpression method:
                return ParseMethod(method, walkerCtx);

            case NegatedExpression negated:
                return ParseNegated(negated, walkerCtx);

            case TrueExpression:
                return BooleanOp.True;

            default:
                throw new InvalidOperationException(
                    $"Unexpected expression type {expr.GetType().Name} in WHERE clause.");
        }
    }

    private static BooleanOp ParseBinaryExpression(BinaryExpression be, ResolutionContext walkerCtx)
    {
        switch (be.Operator)
        {
            case OperatorType.And:
            {
                BooleanOp left = be.Left is BinaryExpression { Operator: OperatorType.Or } ? HandleGroup(be.Left, ClauseType.OrGroup) : ParseExpression(be.Left, walkerCtx);
                BooleanOp right = be.Right is BinaryExpression { Operator: OperatorType.Or } ? HandleGroup(be.Right, ClauseType.OrGroup) : ParseExpression(be.Right, walkerCtx);

                return (left, right) switch
                {
                    (BooleanOp.True, _) => right,
                    (_, BooleanOp.True) => left,
                    (BooleanOp.False, BooleanOp.False) => BooleanOp.False,
                    _ => BooleanOp.And
                };
            }

            case OperatorType.Or:
            {
                BooleanOp left = be.Left is BinaryExpression { Operator: OperatorType.And } ? HandleGroup(be.Left, ClauseType.AndGroup) : ParseExpression(be.Left, walkerCtx);
                BooleanOp right = be.Right is BinaryExpression { Operator: OperatorType.And } ? HandleGroup(be.Right, ClauseType.AndGroup) : ParseExpression(be.Right, walkerCtx);

                return (left, right) switch
                {
                    (BooleanOp.True, _) => BooleanOp.True,
                    (_, BooleanOp.True) => BooleanOp.True,
                    (BooleanOp.False, BooleanOp.False) => BooleanOp.False,
                    _ => BooleanOp.Or
                };
            }

            case OperatorType.Equal:
                ParseComparison(be, walkerCtx);
                return BooleanOp.Leaf;

            case OperatorType.NotEqual:
                ParseComparison(be, walkerCtx);
                return BooleanOp.Leaf;

            case OperatorType.LessThan:
            case OperatorType.LessThanEqual:
            case OperatorType.GreaterThan:
            case OperatorType.GreaterThanEqual:
                ParseRangeComparison(be, walkerCtx);
                return BooleanOp.Leaf;

            default:
                throw new InvalidOperationException(
                    $"Unexpected binary operator {be.Operator} in WHERE clause.");
        }

        BooleanOp HandleGroup(QueryExpression queryExpression, ClauseType clauseType)
        {
            List<ClauseInfo> saved = walkerCtx.Clauses;
            walkerCtx.Clauses = [];
            BooleanOp expr = ParseExpression(queryExpression, walkerCtx);
            List<ClauseInfo> clauses = walkerCtx.Clauses;
            walkerCtx.Clauses = saved;

            ClauseInfo clauseInfo = new() { ClauseType = clauseType, OriginalIndex = walkerCtx.Clauses.Count, SubClauses = clauses };

            walkerCtx.Clauses.Add(clauseInfo);
            return expr;
        }
    }

    private static void ParseComparison(BinaryExpression be, ResolutionContext walkerCtx)
    {
        if (TryGetFieldName(be.Left, walkerCtx, out string fieldName) == false)
        {
            walkerCtx.Report($"Comparison left side must be a field expression or id(), but got: {be.Left.Type}");
            return;
        }

        bool isNotEqual = be.Operator == OperatorType.NotEqual;
        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = isNotEqual ? ClauseType.NotEquals : ClauseType.Equals,
            IsNegated = isNotEqual,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [CreateBinding(be.Right, walkerCtx)]
        });
    }

    private static void ParseRangeComparison(BinaryExpression be, ResolutionContext walkerCtx)
    {
        if (TryGetFieldName(be.Left, walkerCtx, out string fieldName) == false)
        {
            walkerCtx.Report($"Range comparison left side must be a field expression or id(), but got: {be.Left.Type}");
            return;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            Bindings = [CreateBinding(be.Right, walkerCtx)],
            ClauseType = be.Operator switch
            {
                OperatorType.GreaterThan => ClauseType.GreaterThan,
                OperatorType.GreaterThanEqual => ClauseType.GreaterThanOrEqual,
                OperatorType.LessThan => ClauseType.LessThan,
                OperatorType.LessThanEqual => ClauseType.LessThanOrEqual,
                _ => ClauseType.Equals
            },
            OriginalIndex = walkerCtx.Clauses.Count
        });
    }

    private static void ParseBetween(BetweenExpression between, ResolutionContext walkerCtx)
    {
        if (TryGetFieldName(between.Source, walkerCtx, out string resolvedFieldName) == false)
        {
            walkerCtx.Report($"BETWEEN source must be a field expression or id(), but got: {between.Source.Type}");
            return;
        }

        ParameterBinding minBinding = CreateBinding(between.Min, walkerCtx);
        ParameterBinding maxBinding = CreateBinding(between.Max, walkerCtx);

        bool minIsSentinel = minBinding is { LiteralType: ParamValueType.String, LiteralValue: ClientConstants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery };
        bool maxIsSentinel = maxBinding is { LiteralType: ParamValueType.String, LiteralValue: ClientConstants.Documents.Querying.Terms.RightNullValueOfBetweenQuery };
        bool bothAstStrings = between is { Min.Value: ValueTokenType.String, Max.Value: ValueTokenType.String };
        if (!minIsSentinel && !maxIsSentinel
                           && !bothAstStrings
                           && minBinding is { LiteralType: not ParamValueType.Parameter }
                           && maxBinding is { LiteralType: not ParamValueType.Parameter }
                           && minBinding.LiteralType != maxBinding.LiteralType)
        {
            walkerCtx.Report(
                $"BETWEEN bounds for field '{resolvedFieldName}' have different types: " +
                $"low is {minBinding.LiteralType}, high is {maxBinding.LiteralType}. Both must be the same type.");
            return;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            ClauseType = ClauseType.Between,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [minBinding, maxBinding]
        });
    }

    private static void ParseIn(InExpression inExpr, ResolutionContext walkerCtx)
    {
        if (TryGetFieldName(inExpr.Source, walkerCtx, out string resolvedFieldName) == false)
        {
            walkerCtx.Report($"IN source must be a field expression or id(), but got: {inExpr.Source.Type}");
            return;
        }

        if (inExpr.Values.Count == 0)
        {
            walkerCtx.Report("IN/ALL IN with an empty value list is a syntax error.");
            return;
        }

        List<ParameterBinding> inBindings = [];
        foreach (QueryExpression value in inExpr.Values)
        {
            if (CreateBinding(value, walkerCtx) is { } binding)
            {
                inBindings.Add(binding);
            }
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            ClauseType = inExpr.All ? ClauseType.AllIn : ClauseType.In,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = inBindings.ToArray()
        });
    }

    // not(...) must distribute over its operands (De Morgan):
    // not (Name != $p0 and Name != $p1) -> (Name = $p0 or Name = $p1), not(A = 1 or B = 2) -> A !=1 AND B != 2, etc
    private static BooleanOp ParseNegated(NegatedExpression negated, ResolutionContext walkerCtx)
    {
        return negated.Expression switch
        {
            NegatedExpression or BinaryExpression
                {
                    Operator: OperatorType.And or OperatorType.Or or OperatorType.Equal or OperatorType.NotEqual
                } => 
                    ParseExpression(NegationNormalForm(negated.Expression), walkerCtx),
            _ => ParseNegatedLeaf(negated.Expression, walkerCtx)
        };
    }

    private static QueryExpression NegationNormalForm(QueryExpression expr)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        return expr switch
        {
            BinaryExpression { Operator: OperatorType.And } be =>
                new BinaryExpression(NegationNormalForm(be.Left), NegationNormalForm(be.Right), OperatorType.Or) { Parenthesis = true },
            BinaryExpression { Operator: OperatorType.Or } be => 
                new BinaryExpression(NegationNormalForm(be.Left), NegationNormalForm(be.Right), OperatorType.And) { Parenthesis = true },
            BinaryExpression { Operator: OperatorType.Equal } be => 
                new BinaryExpression(be.Left, be.Right, OperatorType.NotEqual),
            BinaryExpression { Operator: OperatorType.NotEqual } be => 
                new BinaryExpression(be.Left, be.Right, OperatorType.Equal),
            NegatedExpression neg => neg.Expression, // not(not(x)) == x
            _ => new NegatedExpression(expr)
        };
    }

    private static BooleanOp ParseNegatedLeaf(QueryExpression inner, ResolutionContext walkerCtx)
    {
        List<ClauseInfo> saved = walkerCtx.Clauses;
        walkerCtx.Clauses = [];
        ParseExpression(inner, walkerCtx);
        List<ClauseInfo> innerClauses = walkerCtx.Clauses;
        walkerCtx.Clauses = saved;
        foreach (ClauseInfo clause in innerClauses)
        {
            clause.IsNegated = !clause.IsNegated;
            walkerCtx.Clauses.Add(clause);
        }
        return BooleanOp.Leaf;
    }

    private delegate BooleanOp MethodHandler(MethodExpression method, ResolutionContext walkerCtx);

    // Dispatch table for the methods allowed inside a WHERE clause. Leaf handlers add a single
    // ClauseInfo (or none, on validation failure) and return BooleanOp.Leaf; wrapper handlers
    // (exact/boost/when) recurse and propagate the inner BooleanOp so that e.g. exact(A OR B) is
    // still detected as OR at the root. Any method not registered here is rejected by ParseMethod.
    private static readonly Dictionary<MethodType, MethodHandler> MethodHandlers = new()
    {
        [MethodType.Search] = ParseSearchMethod,
        [MethodType.StartsWith] = static (m, ctx) => ParseFieldValueLeaf(m, ctx, ClauseType.StartsWith, "field, prefix"),
        [MethodType.EndsWith] = static (m, ctx) => ParseFieldValueLeaf(m, ctx, ClauseType.EndsWith, "field, prefix"),
        [MethodType.Regex] = static (m, ctx) => ParseFieldValueLeaf(m, ctx, ClauseType.Regex, "field, pattern"),
        [MethodType.Exists] = ParseExists,
        [MethodType.Exact] = ParseExact,
        [MethodType.Boost] = ParseBoost,
        [MethodType.When] = ParseWhen,
        [MethodType.Spatial_Within] = ParseSpatial,
        [MethodType.Spatial_Contains] = ParseSpatial,
        [MethodType.Spatial_Disjoint] = ParseSpatial,
        [MethodType.Spatial_Intersects] = ParseSpatial,
        [MethodType.Vector_Search] = ParseVectorSearch,
        // MoreLikeThis in a WHERE clause acts as "all entries" — the actual MLT logic runs in the
        // separate reader.MoreLikeThis() path, so here it is a no-op that matches everything.
        [MethodType.MoreLikeThis] = static (_, _) => BooleanOp.Leaf,
    };

    private static BooleanOp ParseMethod(MethodExpression method, ResolutionContext walkerCtx)
    {
        MethodType methodType = QueryMethod.GetMethodType(method.Name.Value);
        if (MethodHandlers.TryGetValue(methodType, out MethodHandler handler) == false)
            throw new InvalidOperationException($"Unexpected method '{method.Name.Value}' ({methodType}) in WHERE clause.");

        return handler(method, walkerCtx);
    }

    private static BooleanOp ParseExists(MethodExpression method, ResolutionContext walkerCtx)
    {
        if (method.Arguments.Count == 0)
        {
            walkerCtx.Report("exists() requires a field argument.");
            return BooleanOp.Leaf;
        }

        if (TryGetFieldName(method.Arguments[0], walkerCtx, out string existsFieldName) == false)
        {
            walkerCtx.Report($"exists() argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
            return BooleanOp.Leaf;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = existsFieldName,
            ClauseType = ClauseType.Exists,
            OriginalIndex = walkerCtx.Clauses.Count
        });
        return BooleanOp.Leaf;
    }

    // Shared shape for the (field, value) leaf methods: startsWith, endsWith, regex.
    private static BooleanOp ParseFieldValueLeaf(MethodExpression method, ResolutionContext walkerCtx, ClauseType clauseType, string argHint)
    {
        string label = clauseType.ToString().ToLowerInvariant();
        if (method.Arguments.Count < 2)
        {
            walkerCtx.Report($"{label}() requires at least 2 arguments ({argHint}), but got {method.Arguments.Count}.");
            return BooleanOp.Leaf;
        }

        if (TryGetFieldName(method.Arguments[0], walkerCtx, out string fieldName) == false)
        {
            walkerCtx.Report($"{label}() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
            return BooleanOp.Leaf;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = clauseType,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [CreateBinding(method.Arguments[1], walkerCtx)]
        });
        return BooleanOp.Leaf;
    }

    private static BooleanOp ParseSearchMethod(MethodExpression method, ResolutionContext walkerCtx)
    {
        if (method.Arguments.Count < 2)
        {
            walkerCtx.Report($"search() requires at least 2 arguments (field, term), but got {method.Arguments.Count}.");
            return BooleanOp.Leaf;
        }

        if (TryGetFieldName(method.Arguments[0], walkerCtx, out string fieldName) == false)
        {
            walkerCtx.Report($"search() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
            return BooleanOp.Leaf;
        }

        Constants.Search.Operator searchOp = Constants.Search.Operator.Or;
        if (method.Arguments.Count >= 3 && method.Arguments[2] is FieldExpression opField
                                        && opField.Compound.Count == 1)
        {
            string op = opField.Compound[0].Value;
            if (string.Equals("AND", op, StringComparison.OrdinalIgnoreCase))
            {
                searchOp = Constants.Search.Operator.And;
            }
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = ClauseType.Search,
            SearchOperator = searchOp,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [CreateBinding(method.Arguments[1], walkerCtx)]
        });
        return BooleanOp.Leaf;
    }

    private static BooleanOp ParseExact(MethodExpression method, ResolutionContext walkerCtx)
    {
        // exact(expr) → recurse, then mark all new clauses as exact.
        int beforeCount = walkerCtx.Clauses.Count;
        BooleanOp innerOp = BooleanOp.Leaf;
        if (method.Arguments.Count > 0)
        {
            innerOp = ParseExpression(method.Arguments[0], walkerCtx);
        }

        for (int c = beforeCount; c < walkerCtx.Clauses.Count; c++)
        {
            walkerCtx.Clauses[c].IsExact = true;
        }

        return innerOp;
    }

    private static BooleanOp ParseBoost(MethodExpression method, ResolutionContext walkerCtx)
    {
        // boost(expr, factor) → recurse, then capture the clauses' instances for later call to BoostPropagate.
        int beforeCount = walkerCtx.Clauses.Count;
        if (method.Arguments.Count is 0)
        {
            return BooleanOp.Leaf;
        }

        BooleanOp innerOp = ParseExpression(method.Arguments[0], walkerCtx);
        if (method.Arguments.Count is 1 ||
            walkerCtx.Clauses.Count == beforeCount ||
            CreateBinding(method.Arguments[1], walkerCtx) is not { } boostBinding)
        {
            return innerOp;
        }

        List<ClauseInfo> inner = walkerCtx.Clauses.Slice(beforeCount, walkerCtx.Clauses.Count - beforeCount);
        walkerCtx.RecordPendingBoost(inner, boostBinding);
        return innerOp;
    }

    private static BooleanOp ParseWhen(MethodExpression method, ResolutionContext walkerCtx)
    {
        QueryExpression conditionExpr = method.Arguments[0];
        int beforeCount = walkerCtx.Clauses.Count;
        BooleanOp innerOp = ParseExpression(method.Arguments[1], walkerCtx);
        var whenCondition = new WhenConditionEvaluator(conditionExpr, walkerCtx.Metadata).Evaluate;
        for (int wi = beforeCount; wi < walkerCtx.Clauses.Count; wi++)
        {
            walkerCtx.Clauses[wi].WhenCondition = whenCondition;
        }

        return innerOp;
    }

    private static BooleanOp ParseSpatial(MethodExpression method, ResolutionContext walkerCtx)
    {
        MethodType methodType = QueryMethod.GetMethodType(method.Name.Value);
        if (method.Arguments is [_, not MethodExpression, ..])
        {
            walkerCtx.Report($"Spatial shape argument must be a method expression (spatial.circle or spatial.wkt), but got: {method.Arguments[1].Type}");
            return BooleanOp.Leaf;
        }

        // Capture bindings for all spatial sub-arguments.
        // Shape type and field name are structural; parameter values resolved per-execution.
        string spatialFieldName;
        if (walkerCtx.Metadata.IsDynamic && method.Arguments[0] is MethodExpression spatialPointExpr)
        {
            spatialFieldName = walkerCtx.Metadata.GetSpatialFieldName(spatialPointExpr, walkerCtx.QueryParameters);
        }
        else if (TryGetFieldName(method.Arguments[0], walkerCtx, out string sfn))
        {
            spatialFieldName = sfn;
        }
        else
        {
            spatialFieldName = QueryBuilderHelper.ExtractIndexFieldName(walkerCtx.Metadata.Query, walkerCtx.QueryParameters, method.Arguments[0], walkerCtx.Metadata);
        }

        MethodExpression shapeExpr = (MethodExpression)method.Arguments[1];
        MethodType shapeType = QueryMethod.GetMethodType(shapeExpr.Name.Value);

        // Build spatial bindings: [0]=distErrPct, then shape-specific args
        List<ParameterBinding> spatialBindings =
        [
            method.Arguments.Count == 3
                ? CreateBinding(method.Arguments[2], walkerCtx)
                : null
        ];

        switch (shapeType, shapeExpr.Arguments.Count)
        {
            case (MethodType.Spatial_Circle, >= 3):
                spatialBindings.Add(CreateBinding(shapeExpr.Arguments[0], walkerCtx)); // radius
                spatialBindings.Add(CreateBinding(shapeExpr.Arguments[1], walkerCtx)); // lat
                spatialBindings.Add(CreateBinding(shapeExpr.Arguments[2], walkerCtx)); // lng
                spatialBindings.Add(shapeExpr.Arguments.Count == 4 // units (optional)
                    ? CreateBinding(shapeExpr.Arguments[3], walkerCtx)
                    : null);
                break;
            case (MethodType.Spatial_Wkt, >= 1):
                spatialBindings.Add(CreateBinding(shapeExpr.Arguments[0], walkerCtx)); // wkt
                spatialBindings.Add(shapeExpr.Arguments.Count == 2 // units (optional)
                    ? CreateBinding(shapeExpr.Arguments[1], walkerCtx)
                    : null);
                break;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = spatialFieldName,
            ClauseType = ClauseType.Spatial,
            SpatialMethodType = ToSpatialOp(methodType),
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = spatialBindings.ToArray()
        });
        return BooleanOp.Leaf;
    }

    private static BooleanOp ParseVectorSearch(MethodExpression method, ResolutionContext walkerCtx)
    {
        // Capture bindings for vector sub-arguments.
        // Resolve field name (structural — uses metadata for dynamic index field naming).
        string vectorFieldName = walkerCtx.Metadata.IsDynamic
            ? walkerCtx.Metadata.GetVectorFieldName(method, walkerCtx.QueryParameters)
            : QueryBuilderHelper.ExtractIndexFieldName(walkerCtx.Metadata.Query, walkerCtx.QueryParameters, method.Arguments[0], walkerCtx.Metadata);

        VectorSourceKind vecMethod = VectorSourceKind.Inline;
        ParameterBinding vectorValueBinding = null;
        ParameterBinding aiTaskBinding = null;

        QueryExpression srcVector = method.Arguments[1];
        if (srcVector is MethodExpression methodValue)
        {
            vecMethod = methodValue.Name.ToString() switch
            {
                ClientConstants.VectorSearch.EmbeddingForDocument => VectorSourceKind.FromDocument,
                ClientConstants.VectorSearch.EmbeddingForRaw => VectorSourceKind.Inline,
                ClientConstants.VectorSearch.EmbeddingText => VectorSourceKind.FromText,

                _ => VectorSourceKind.Inline
            };
            if (methodValue.Arguments.Count > 0)
            {
                vectorValueBinding = CreateBinding(methodValue.Arguments[0], walkerCtx);
            }

            if (vecMethod == VectorSourceKind.FromText && methodValue.Arguments.Count > 1
                                                       && methodValue.Arguments[1] is MethodExpression aiMethod && aiMethod.Arguments.Count > 0)
            {
                aiTaskBinding = CreateBinding(aiMethod.Arguments[0], walkerCtx);
            }
        }
        else
        {
            vectorValueBinding = CreateBinding(srcVector, walkerCtx);
        }

        ParameterBinding minimumMatchBinding = method.Arguments.Count <= 2 ? null : CreateBinding(method.Arguments[2], walkerCtx);
        ParameterBinding numberOfCandidatesBinding = method.Arguments.Count <= 3 ? null : CreateBinding(method.Arguments[3], walkerCtx);
        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = vectorFieldName,
            ClauseType = ClauseType.Vector,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [vectorValueBinding, minimumMatchBinding, numberOfCandidatesBinding, aiTaskBinding],
            VectorMethod = vecMethod
        });
        return BooleanOp.Leaf;
    }

    private static string GetFieldName(FieldExpression field, QueryMetadata metadata, BlittableJsonReaderObject queryParameters)
    {
        return metadata != null ? metadata.GetIndexFieldName(field, queryParameters).Value : field.FieldValue;
    }

    private static bool TryGetFieldName(QueryExpression expr, ResolutionContext ctx, out string fieldName)
    {
        fieldName = expr switch
        {
            FieldExpression fe => GetFieldName(fe, ctx.Metadata, ctx.QueryParameters),
            // Quoted field names (e.g. 'Order' for reserved words) are parsed as ValueExpression
            ValueExpression ve when ve.GetValue(ctx.QueryParameters)?.ToString() is { } resolved =>
                ctx.Metadata != null
                    ? ctx.Metadata.GetIndexFieldName(new QueryFieldName(resolved, ve.Value == ValueTokenType.String), ctx.QueryParameters).Value
                    : resolved,
            MethodExpression me when string.Equals(me.Name.Value, "id", StringComparison.OrdinalIgnoreCase) =>
                ClientConstants.Documents.Indexing.Fields.DocumentIdFieldName,
            _ => null
        };
        return fieldName != null;
    }

    private static (object Value, ValueTokenType Type) ResolveParameterValue(object value)
    {
        Debug.Assert(value is not BlittableJsonReaderArray and not BlittableJsonReaderObject,
            $"ResolveParameterValue called with non-scalar type {value?.GetType().Name}. " +
            "Caller must handle arrays/objects before calling this method.");

        switch (value)
        {
            case null:
                return (null, ValueTokenType.String);
            case bool b:
                return (b ? "true" : "false", ValueTokenType.String);
            case long l:
                return (l, ValueTokenType.Long);
            case int i:
                return ((long)i, ValueTokenType.Long);
            case double d:
                return (d, ValueTokenType.Double);
            case float f:
                return ((double)f, ValueTokenType.Double);
            case decimal dec:
                return ((double)dec, ValueTokenType.Double);
            case DateTime dt:
                return (dt.Ticks, ValueTokenType.Long);
            case DateTimeOffset dto:
                return (dto.UtcDateTime.Ticks, ValueTokenType.Long);
            case LazyNumberValue lnv when lnv.TryParseLong(out long lnvLong):
                return (lnvLong, ValueTokenType.Long);
            case LazyNumberValue lnv:
                return ((double)lnv, ValueTokenType.Double);
            default:
            {
                string str = value.ToString();
                if (str is { Length: > 18 and < 35 } && str.Contains('T')
                                                     && DateTime.TryParse(str, CultureInfo.InvariantCulture,
                                                         DateTimeStyles.RoundtripKind, out DateTime parsed))
                {
                    return (parsed.Ticks, ValueTokenType.Long);
                }

                return (str, ValueTokenType.String);
            }
        }
    }

    private static ParameterBinding CreateBinding(QueryExpression expr, ResolutionContext ctx)
    {
        if (BuildBinding(expr, ctx) is not { } binding) 
            return null;
        
        binding.ValueOrdinal = ctx.SlotBindings.Count;
        ctx.SlotBindings.Add(binding);
        return binding;
    }

    private static ParameterBinding BuildBinding(QueryExpression expr, ResolutionContext ctx)
    {
        switch (expr)
        {
            case MethodExpression me:
                // Method expressions like cmpxchg(), now(), today() must be resolved at execution time
                return new ParameterBinding
                {
                    Source = BindingSource.DeferredMethod,
                    DeferredExpression = (builderParamsObj, qp) =>
                    {
                        QueryBuilderParameters bp = (QueryBuilderParameters)builderParamsObj;
                        QueryExpression resolvedExpr = QueryBuilderHelper.EvaluateMethod(
                            bp.Query.Metadata.Query,
                            bp.Metadata,
                            bp.ServerContext,
                            bp.DocumentsContext.DocumentDatabase.CompareExchangeStorage,
                            me, qp, bp.QueryTime);
                        
                        if (resolvedExpr is not ValueExpression valueExpression || valueExpression.Value == ValueTokenType.Null)
                            return null;

                        return valueExpression.GetValue(qp);
                    },
                    LiteralType = ParamValueType.String
                };
            case ValueExpression ve:
                if (ve.Value == ValueTokenType.Parameter)
                    return new ParameterBinding { Source = BindingSource.QueryParameter, ParameterName = ve.Token.Value, LiteralType = ParamValueType.Parameter };

                object value = ve.GetValue(ctx.QueryParameters);

                if (ve.Value == ValueTokenType.Null || value is null)
                    return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = null, LiteralType = ParamValueType.String };

                if (value is bool b)
                    return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = b ? "true" : "false", LiteralType = ParamValueType.String };

                (object resolved, ValueTokenType resolvedType) = ResolveParameterValue(value);
                return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = resolved, LiteralType = ToParamValueType(resolvedType) };
            default:
                return null;
        }
    }

}
