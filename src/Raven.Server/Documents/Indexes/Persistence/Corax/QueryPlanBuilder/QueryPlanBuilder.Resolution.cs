using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Impl;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static PlanTemplate BuildTemplate(PlanParameters planParams)
    {
        var planCache = planParams.IndexSearcher.PlanCache;
        var metadata = planParams.Metadata;

        var generation = planCache.GenerationIdx;

        if (metadata.CachedPlanMemo is { } memo
            && memo.PlanCacheGeneration == generation
            && memo.Bucket.TryGetTarget(out var warmBucket))
        {
            return Finalize(warmBucket);
        }

        var structuralKey = ComputeStructuralKey(planParams);
        if (planCache.GetBucket(structuralKey) is { } existing)
        {
            metadata.CachedPlanMemo = new QueryMetadata.PlanMemo(generation, existing);
            return Finalize(existing);
        }

        var template = ParseTemplate(planParams);
        template.SortMetadataTemplate = BuildSortMetadataTemplate(planParams, template);
        var bucket = planCache.GetOrAddBucket(structuralKey, template, planParams.CacheKey);
        metadata.CachedPlanMemo = new QueryMetadata.PlanMemo(generation, bucket);
        return Finalize(bucket);

        PlanTemplate Finalize(PlanCache.PerQueryPlans b)
        {
            // ExtractSlotBindings is only called on fresh metadata instances (rare, cached at db level)
            var bindings = metadata.CachedSlotBindings ??= ExtractSlotBindings(planParams);
            planParams.SlotBindings = bindings;
            planParams.Bucket = b;
            AssertSlotBindingsMatchTemplate(b.Template, bindings);
            return b.Template;
        }
    }

    [Conditional("DEBUG")]
    private static void AssertSlotBindingsMatchTemplate(PlanTemplate template, ParameterBinding[] slotBindings)
    {
        Debug.Assert(template.ValueOrdinalCount == slotBindings.Length,
            $"Slot-binding vector length ({slotBindings.Length}) must equal the template value-ordinal count " +
            $"({template.ValueOrdinalCount}). Both come from the same canonical WHERE walk, so a mismatch means the " +
            "template parse and the per-query slot-vector parse diverged.");
    }

    /// <summary>
    /// This gets the query match without any sorting. For facets, more-like-this, etc.
    /// </summary>
    public static IQueryMatch BuildFilterMatch(PlanParameters planParams, QueryBuilderParameters builderParameters, Dictionary<string, 
            CoraxHighlightingTermIndex> highlightingTerms, bool wantTimings, CancellationToken token)
    {
        var walkerCtx = new ResolutionContext(builderParameters);

        var template = BuildTemplate(planParams);

        var exec = new BuildResolver(template, planParams, builderParameters, walkerCtx).Resolve();
        return InstantiateBitmapPipeline(exec.Plan, exec, planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, token);
    }

    public static CompiledQuery BuildSortedQuery(PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var walkerCtx = new ResolutionContext(builderParameters);

        var template = BuildTemplate(planParams);

        var exec = new BuildResolver(template, planParams, builderParameters, walkerCtx).Resolve();
        var orderByFields = GetSortMetadata(builderParameters, exec.Plan.Template);
        // A single vector-search post-filter already streams its output in score order, we can skip the sorting step then
        exec.VectorPostFilterProvidesScoreOrder = VectorPostFilterProvidesResultOrder(exec, builderParameters, orderByFields);
        var (queryMatch,  innerMatch) = Instantiate(exec, orderByFields, planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, token);
        return new(queryMatch, innerMatch, queryMatch == innerMatch ? null : queryMatch, exec, builderParameters, orderByFields);
    }

    internal static ClauseExecution CreateExecution(ClauseInfo clause)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var exec = new ClauseExecution(clause);

        if (clause.SubClauses is null)
            return exec;

        exec.SubExecutions = new List<ClauseExecution>(clause.SubClauses.Count);
        foreach (var it in clause.SubClauses)
        {
            exec.SubExecutions.Add(CreateExecution(it));
        }

        return exec;
    }

    /// <summary>Set the bit for a parameter-bound BETWEEN sentinel's slot, forcing a distinct plan-cache entry.</summary>
    private static void MarkSentinel(Span<ulong> sentinelBits, ParameterBinding binding)
    {
        int slot = binding.ParameterSlot;
        if (slot < 0 || sentinelBits.IsEmpty)
            return;
        sentinelBits[slot >> 6] |= 1UL << (slot & 63);
    }

    internal static void PopulateClauseValues(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, ValueWriter writer, QueryBuilderParameters builderParameters,
        Span<ulong> sentinelBits)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        foreach (var it in exec.SubExecutions ?? [])
        {   // Always recurse into subclauses first (OrGroup/AndGroup have no binding of their own)
            PopulateClauseValues(it, slotBindings, queryParameters, writer, builderParameters, sentinelBits);
        }

        if (exec.Clause is { HasBoost: true, Bindings.Length: > 0 })
        {
            ResolveBoostFactor(exec, slotBindings, queryParameters, builderParameters);
        }

        switch (exec.Clause.ClauseType) // Spatial and vector resolve via their binding array.
        {
            case ClauseType.Spatial when exec.Clause.Bindings is { Length: > 0 }:
                ResolveSpatialFromBindings(exec, slotBindings, queryParameters, builderParameters);
                return;
            case ClauseType.Vector when exec.Clause.Bindings is { Length: > 0 }:
                ResolveVectorFromBindings(exec, slotBindings, queryParameters);
                return;
        }

        if (exec.Clause.Bindings is not { Length: > 0 })
            return;

        var bindings = exec.Clause.Bindings;
        switch (exec.Clause.ClauseType)
        {
            case ClauseType.Between: // BETWEEN: open-range "*"/"NULL" sentinel bounds (literal or parameter-bound) are detected here and rewritten to the equivalent half-open range / match-all leaf.
            {
                var (low, lowType) = ResolveBindingScalar(bindings[BindingIndex.BetweenLow], slotBindings, queryParameters, builderParameters);
                var (high, highType) = ResolveBindingScalar(bindings[BindingIndex.BetweenHigh], slotBindings, queryParameters, builderParameters);
                (low, lowType) = ToTicksIfFieldHasTimeValues(exec, low, lowType, builderParameters);
                (high, highType) = ToTicksIfFieldHasTimeValues(exec, high, highType, builderParameters);
                bool lowIsSentinel = low is RavenConstants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery;
                bool highIsSentinel = high is RavenConstants.Documents.Querying.Terms.RightNullValueOfBetweenQuery;
                switch (lowIsSentinel, highIsSentinel)
                {
                    case (true, true):
                        exec.SentinelRewriteType = ClauseType.Exists;
                        MarkSentinel(sentinelBits, bindings[BindingIndex.BetweenLow]);
                        MarkSentinel(sentinelBits, bindings[BindingIndex.BetweenHigh]);
                        return;
                    case (true, false):
                        exec.SentinelRewriteType = ClauseType.LessThanOrEqual;
                        MarkSentinel(sentinelBits, bindings[BindingIndex.BetweenLow]);
                        exec.TermValueType = highType;
                        exec.PackedParamValue = writer.Add(high, highType);
                        return;
                    case (false, true):
                        exec.SentinelRewriteType = ClauseType.GreaterThanOrEqual;
                        MarkSentinel(sentinelBits, bindings[BindingIndex.BetweenHigh]);
                        exec.TermValueType = lowType;
                        exec.PackedParamValue = writer.Add(low, lowType);
                        return;
                    case (false, false):
                        exec.TermValueType = lowType;
                        exec.PackedParamValue = writer.AddPair(low, high, lowType);
                        return;
                }
            }
            case ClauseType.In or ClauseType.AllIn:
                Span<ParameterBinding> inBindings =  bindings;
                if(exec.Clause.HasBoost)
                {   // Boosted clauses store the boost factor in the trailing binding (read by ResolveBoostFactor via Bindings[^1]); exclude it from the IN-term walk.
                    inBindings = inBindings[..^1];
                }
                ResolveInFromBindings(exec, slotBindings, queryParameters, writer, inBindings, builderParameters);
                break;
            default: // Simple clause (Equals, Range, Search, Regex, etc.): single value at Bindings[0]
                var (value, valueType) = ResolveBindingScalar(bindings[BindingIndex.Value], slotBindings, queryParameters, builderParameters);
                (value, valueType) = ToTicksIfFieldHasTimeValues(exec, value, valueType, builderParameters);
                if (value == null && exec.Clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith or ClauseType.Search or ClauseType.Regex)
                {
                    throw new InvalidQueryException(  // reject null (matches Lucene behavior).
                        $"Method {exec.Clause.ClauseType}() expects to get an argument of type String while it got Null");
                }

                exec.TermValueType = valueType;
                exec.PackedParamValue = writer.Add(value, valueType);
                break;
        }
    }


    private static void EmitInTerms(ClauseExecution exec, ValueWriter writer, ParamValueType dominantType, List<object> values, bool hasNullTerm)
    {
        var (packedType, startIdx) = writer.ResolveInSlot(dominantType);

        int written = 0;
        for (int i = 0; i < values.Count; i++)
        {
            // Mixed-type IN: (IN [long, "Shalom"]). Silently drop it instead of throwing, Matches Lucene's behavior.
            if (writer.TryAdd(values[i], dominantType) is null)
                continue;
            written++;
        }

        exec.PackedParamValue = new PackedParam(packedType, startIdx);
        exec.InTermCount = written;
        exec.HasNullTerm = hasNullTerm;
    }

    // A date-shaped value becomes ticks only when the field actually holds time values - the same check Corax 1.0
    // does before converting. Without it a string field is searched with a long term and matches nothing.
    private static (object Value, ParamValueType Type) ToTicksIfFieldHasTimeValues(ClauseExecution exec, object value, ParamValueType type,
        QueryBuilderParameters builderParameters)
    {
        if (type != ParamValueType.String || value == null || builderParameters == null)
            return (value, type);

        if (exec.Clause.ClauseType is not (ClauseType.Equals or ClauseType.NotEquals
            or ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
            or ClauseType.LessThan or ClauseType.LessThanOrEqual
            or ClauseType.Between or ClauseType.In or ClauseType.AllIn))
            return (value, type);

        if (exec.Clause.FieldName is not { } fieldName
            || builderParameters.Index is not { } index
            || index.IndexFieldsPersistence.HasTimeValues(fieldName) == false
            || QueryBuilderHelper.TryGetTime(index, value, out long ticks) == false)
            return (value, type);

        return (ticks, ParamValueType.Long);
    }

    private static (object Value, ParamValueType Type) ResolveBindingScalar(ParameterBinding binding, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters)
    {
        // switch to the binding for the _current_ query...
        binding = slotBindings[binding.ValueOrdinal];
        switch (binding.Source)
        {
            case BindingSource.Literal:
                return (binding.LiteralValue, binding.LiteralType);

            case BindingSource.DeferredMethod:
            {
                var value = binding.DeferredExpression(builderParameters, queryParameters);
                if (value == null)
                    return (null, ParamValueType.Null);
                var (val, valType) = ResolveParameterValue(value);
                return (val, ToParamValueType(valType));
            }

            case BindingSource.QueryParameter:
            default:
                if (queryParameters == null) // query text references $param but no parameters were supplied
                    QueryBuilderHelper.ThrowParametersWereNotProvided(builderParameters?.Metadata?.QueryText);

                if (queryParameters.TryGet(binding.ParameterName, out object raw) == false) // referenced parameter is absent from the supplied set
                    QueryBuilderHelper.ThrowParameterValueWasNotProvided(binding.ParameterName, builderParameters?.Metadata?.QueryText, queryParameters);

                if (raw == null) // explicit null value is allowed (matches null terms)
                    return (null, ParamValueType.Null);

                var (paramVal, paramType) = ResolveParameterValue(raw);
                return (paramVal, ToParamValueType(paramType));
        }
    }

    private static void ResolveBoostFactor(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters)
    {
        var (boostVal, boostType) = ResolveBindingScalar(exec.Clause.Bindings[^1], slotBindings, queryParameters, builderParameters);
        if (boostVal == null) return;

        exec.BoostFactor = boostType switch
        {
            ParamValueType.Double => (float)(double)boostVal,
            _ => boostType switch
            {
                ParamValueType.Long => (long)boostVal,
                _ when float.TryParse(boostVal.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out float parsed) => parsed,
                _ => 1f
            }
        };
    }

    /// <summary> Foo BETWEEN $x AND $y - where $x > $y - returns nothing</summary>
    internal static void PropagateBetweenContradiction(ClauseExecution exec, ValueWriter writer, long numberOfEntries)
    {
        var p = exec.PackedParamValue;
        if (exec.Clause.ClauseType != ClauseType.Between || p.Param2 is PackedParam.NoParamValue)
            return;

        bool contradictory = p.ValueType switch
        {
            PackedParam.TypeLong => writer.GetLong(p.Param1) > writer.GetLong(p.Param2),
            PackedParam.TypeDouble => writer.GetDouble(p.Param1) > writer.GetDouble(p.Param2),
            _ => false // for strings, we have to consider analyzers, so we can't tell
        };
        if (!contradictory)
            return;

        exec.MarkAsResolvedSentinel(ClauseType.MatchNothing, numberOfEntries);
    }

    private static IQueryMatch InstantiateBitmapPipeline(
        CompiledPlan compiledPlan,
        QueryExecution exec,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        ResolutionContext walkerCtx,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;

        // Spatial / Vector queries with no other clauses ( WHERE spatial.within() / WHERE vector.search() )
        // use a dedicated code path to avoid AllEntries + post-filters
        if (exec is { IsAllEntries: true, HasSpatialOrVector: true })
            return InstantiateAllEntriesPostFilter(exec, builderParameters, walkerCtx, wantTimings);

        var (resolvedMatches, leaves) = ResolveAllSlots(exec, walkerCtx, planParams.HasBoost);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(exec, highlightingTerms, planParams.Metadata);

        var compiledMatch = new CompiledQueryMatch(
            compiledPlan, exec, compiledPlan.RequiredBitmaps, compiledPlan.OpCount, resolvedMatches, leaves,
            indexSearcher, planParams.Allocator, wantTimings, token)
        {
            InRangeCounts = exec.InRangeCounts,
            Cardinalities = exec.Cardinalities,
        };

        if (exec.Plan.EntryScanSet is { HasPredicates: true })
        {
            exec.PopulateScanParams = () => ScanParamExtractor.Extract(exec, indexSearcher, walkerCtx, exec.Plan.EntryScanSet);
        }

        IQueryMatch[] spatialMatches = null;
        if (exec.SpatialFilters is { Length: > 0 })
        {
            spatialMatches = new IQueryMatch[exec.SpatialFilters.Length];
            for (int sf = 0; sf < exec.SpatialFilters.Length; sf++)
                spatialMatches[sf] = resolvedMatches[exec.SpatialFilters[sf].MatchIndex];
        }

        return ApplyPostFilters(compiledMatch, spatialMatches, exec, builderParameters, wantTimings);
    }

    private static IQueryMatch ApplyPostFilters(
        IQueryMatch source, IQueryMatch[] spatialMatches,
        QueryExecution exec, QueryBuilderParameters builderParameters, bool wantTimings)
    {
        IQueryMatch result = source;

        // Negated post-filters (spatial and vector) are collected as factories: given the materialized candidate
        // universe R, each returns its positive clause scoped to R. NegatedPostFilterMatch subtracts them globally.
        List<Func<IQueryMatch, IQueryMatch>> negatedFactories = null;

        if (spatialMatches is { Length: > 0 })
        {
            List<IQueryMatch> positiveSpatial = null;
            for (int sf = 0; sf < spatialMatches.Length; sf++)
            {
                var sm = spatialMatches[sf];
                if (sm is IPostFilterMatch postFilter)
                    postFilter.IsPostFilter = true;

                if (exec.SpatialFilters[sf].Clause.IsNegated)
                {
                    negatedFactories ??= new List<Func<IQueryMatch, IQueryMatch>>();
                    var spatial = sm; // capture per-iteration
                    negatedFactories.Add(filter =>
                    {
                        ((ISpatialFilterQuery)spatial).FilterQuery = filter;
                        return spatial;
                    });
                }
                else
                {
                    positiveSpatial ??= new List<IQueryMatch>();
                    positiveSpatial.Add(sm);
                }
            }

            if (positiveSpatial is { Count: > 0 })
            {
                var arr = positiveSpatial.ToArray();
                result = result is null
                    ? new PostFilterMatch(arr[0], arr.Length == 1 ? [] : arr[1..], wantTimings)
                    : new PostFilterMatch(result, arr, wantTimings);
            }
        }

        if (exec.VectorSelects is { Length: > 0 })
        {
            foreach (var item in ResolveVectorItems(exec, builderParameters))
            {
                if (item.IsNegated)
                {
                    negatedFactories ??= new List<Func<IQueryMatch, IQueryMatch>>();
                    var vec = item; // capture per-iteration
                    negatedFactories.Add(filter =>
                    {
                        // CoraxVectorItem.IsNegated is the routing signal only; the wrapper does the subtraction,
                        // so the clause itself must materialize its positive (matching) results scoped to filter.
                        return vec.Materialize(filter, isPostFilter: true);
                    });
                }
                else
                {
                    result = item.Materialize(result, isPostFilter: true, streamScoreOrder: exec.VectorPostFilterProvidesScoreOrder);
                }
            }
        }

        if (negatedFactories is { Count: > 0 })
        {
            // A pure-negated query has no positive universe — subtract from every entry.
            result ??= builderParameters.IndexSearcher.AllEntries();
            result = new NegatedPostFilterMatch(builderParameters.IndexSearcher, result, negatedFactories.ToArray(), builderParameters.Token);
        }

        return result;
    }

    /// <summary>
    /// Bypass path for queries with no real WHERE clauses — only spatial filters and/or  vector selects.
    /// </summary>
    private static IQueryMatch InstantiateAllEntriesPostFilter(QueryExecution exec, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx, bool wantTimings)
    {
        // No real WHERE clause, so the spatial clauses aren't in resolvedMatches — resolve them directly.
        IQueryMatch[] spatialMatches = null;
        if (exec.SpatialFilters is { Length: > 0 })
        {
            spatialMatches = new IQueryMatch[exec.SpatialFilters.Length];
            for (int i = 0; i < exec.SpatialFilters.Length; i++)
                spatialMatches[i] = ResolveClause(exec.SpatialFilters[i].Exec, exec, walkerCtx);
        }

        return ApplyPostFilters(source: null, spatialMatches, exec, builderParameters, wantTimings);
    }
    
    public static IQueryMatch BuildQueryForMoreLikeThis(QueryBuilderParameters builderParams, MethodExpression mltCall, QueryExpression expression)
    {
        mltCall.MoreLikeThisExpression ??= CreateQueryMetadataForMoreLikeThis();
        return BuildFilterMatch(new PlanParameters
        {
            IndexSearcher = builderParams.IndexSearcher,
            Metadata =  mltCall.MoreLikeThisExpression,
            QueryParameters = builderParams.QueryParameters,
            Index = builderParams.Index,
            IndexFieldsMapping = builderParams.IndexFieldsMapping,
            Allocator = builderParams.Allocator,
            HasDynamics = builderParams.HasDynamics,
            DynamicFields = builderParams.DynamicFields,
            HasBoost = builderParams.HasBoost,
        }, builderParams, highlightingTerms: null, wantTimings: false, builderParams.Token);

        QueryMetadata CreateQueryMetadataForMoreLikeThis()
        {
            // The base-document sub-expression is compiled as its own standalone query rather than as a special case grafted onto the outer query.
            // We clone the outer query, swap in just this WHERE and drop ORDER BY (the result is an unsorted filter), and build a fresh QueryMetadata for it.
            var subQuery = builderParams.Query.Metadata.Query.ShallowCopy();
            subQuery.Where = expression;
            subQuery.OrderBy = null;
            return new QueryMetadata(subQuery, builderParams.QueryParameters, cacheKey: 0, addSpatialProperties: false);
        }
    }

    
    // the compound writer stores null and "" alike, as an empty component, so neither can be matched there
    private static bool IsNullOrMissingValue(QueryExecution exec, PackedParam packed) =>
        packed.IsNone || (packed.ValueType == PackedParam.TypeString && exec.StringValues[packed.Param1] is null or "");

    private static bool TryCreateCompoundExactMatch(ref InstantiateContext ctx, out string rejectReason)
    {
        // The only thing still unknown is value-dependent — a value can resolve to "none" (missing) or to null, neither of which has a composite-key encoding.
        if (IsNullOrMissingValue(ctx.Exec, ctx.Exec.CompoundExactFirst.PackedParamValue) ||
            IsNullOrMissingValue(ctx.Exec, ctx.Exec.CompoundExactSecond.PackedParamValue))
        {
            rejectReason = "the combined-key lookup needs both values, but one is null or missing";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static IQueryMatch ConstructCompoundExact(ref InstantiateContext ctx)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var eA = ctx.Exec.CompoundExactFirst;
        var eB = ctx.Exec.CompoundExactSecond;

        var (firstField, secondField, firstExec, secondExec) = ctx.Exec.Plan.Template.CompoundExactAFirst
            ? (eA.Clause.ResolvedFieldName ?? eA.Clause.FieldName, eB.Clause.ResolvedFieldName ?? eB.Clause.FieldName, eA, eB)
            : (eB.Clause.ResolvedFieldName ?? eB.Clause.FieldName, eA.Clause.ResolvedFieldName ?? eA.Clause.FieldName, eB, eA);
        
        if (TryGetCompoundFieldEncoding(ref ctx, firstField, firstExec.PackedParamValue, firstExec.PackedParamValue.Param1, out var enc1) == false || 
            TryGetCompoundFieldEncoding(ref ctx, secondField, secondExec.PackedParamValue, secondExec.PackedParamValue.Param1, out var enc2) == false)
            return null;

        int totalLen = enc1.Size + enc2.Size + 1;
        if (totalLen > Constants.Terms.MaxLength) 
            return null;

        ctx.PlanParams.Allocator.Allocate(totalLen, out ByteString keyBuf);
        var keySpan = keyBuf.ToSpan();
        var compoundNumericXorMask = ctx.BuilderParams.CompoundFieldNumericXorMask;
        WriteCompoundFieldEncoding(keySpan.Slice(0, enc1.Size), enc1, ctx.Exec, compoundNumericXorMask);
        WriteCompoundFieldEncoding(keySpan.Slice(enc1.Size, enc2.Size), enc2, ctx.Exec, compoundNumericXorMask);
        keySpan[totalLen - 1] = (byte)enc1.Size;

        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(ctx.Exec.Plan.Template.CompoundExactName, hasBoost: false);

        return indexSearcher.TermQuery(compoundFieldMeta, new Slice(keyBuf));
    }

    private static bool TryCreateCompoundFieldMatch(ref InstantiateContext ctx, out string rejectReason)
    {
        if (ctx.Exec.CompoundFieldDrivingClause is null || ctx.Exec.Plan.Template.CompoundFieldSortName is null)
        {
            rejectReason = "no compound field matches this query's filter-and-sort shape";
            return false;
        }

        // Value-dependent, so it can only be checked here: the filter's value is encoded into the compound-field
        // prefix we seek with, and null has no such encoding.
        if (IsNullOrMissingValue(ctx.Exec, ctx.Exec.CompoundFieldDrivingClause.PackedParamValue))
        {
            rejectReason = "the compound-field scan needs the filter's value, but it is null or missing";
            return false;
        }

        if (ctx.Exec.Plan.AllNegated)
        {
            rejectReason = "every filter is a negation (not/!=), so there is no term to drive the scan";
            return false;
        }

        var driving = ctx.Exec.CompoundFieldDrivingClause;
        var field2Range = ctx.Exec.CompoundFieldField2Range;
        var execs = ctx.Exec.Executions;
        foreach (var exec in execs)
        {
            if (ReferenceEquals(exec, driving) || ReferenceEquals(exec, field2Range))
                continue;
            if (IsClauseBoosted(exec))
            {
                rejectReason = "a filter uses boosting, which needs scoring this scan can't do";
                return false;
            }
        }

        if (ctx.Exec.Plan.CompoundFieldResidualSet is null)
        {
            rejectReason = "a filter can't be checked per-document during the scan";
            return false;
        }

        if (field2Range is null && ctx.OrderByFields is [var sortField])
        {
            if (sortField.MayHaveMissingEntries ||
                ctx.PlanParams.IndexSearcher.TryGetPostingListForNull(in sortField.Field, out _))
            {
                rejectReason = "the sort field has null/missing values and no range filter to exclude them, so the scan would order nulls wrong";
                return false;
            }
        }

        rejectReason = null;
        return true;
    }

    private static Slice BuildField1Prefix(ref InstantiateContext ctx, string field1Name, PackedParam packed, out string field1ValueStrForIntrospection)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                field1ValueStrForIntrospection = ctx.Exec.StringValues[packed.Param1];
                var field1Meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, field1Name, hasBoost: false);
                return ctx.Exec.GetAnalyzedSlice(indexSearcher, field1Meta, packed.Param1);
            }
            case PackedParam.TypeLong:
            {
                // skip the ToString allocation unless this is an inspected query.
                field1ValueStrForIntrospection = ctx.WantTimings ? ctx.Exec.LongValues[packed.Param1].ToString() : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeLong, packed.Param1, ctx.Exec, ctx.BuilderParams.CompoundFieldNumericXorMask);
                return new Slice(buf);
            }
            case PackedParam.TypeDouble:
            {
                field1ValueStrForIntrospection = ctx.WantTimings ? ctx.Exec.DoubleValues[packed.Param1].ToString(CultureInfo.InvariantCulture) : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeDouble, packed.Param1, ctx.Exec, ctx.BuilderParams.CompoundFieldNumericXorMask);
                return new Slice(buf);
            }
            default:
                field1ValueStrForIntrospection = null;
                return default;
        }
    }

    private static bool TryCreateSimpleFieldDirectScan(ref InstantiateContext ctx, out string rejectReason)
    {
        if (ctx.OrderByFields is not { Length: > 0 })
        {
            rejectReason = "the query has no ORDER BY for the scan to follow";
            return false;
        }

        if (ctx.OrderByFields.Length > 2)
        {
            rejectReason = "ORDER BY has more than 2 fields (a direct scan supports at most 2)";
            return false;
        }

        bool hasTieBreak = ctx.OrderByFields.Length == 2;
        if (hasTieBreak)
        {
            var tieBreakType = ctx.OrderByFields[1].FieldType;
            if (tieBreakType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
            {
                rejectReason = "the secondary (tie-break) ORDER BY field isn't a number or string";
                return false;
            }
        }

        var execs = ctx.Exec.Executions;
        bool isFullScan = execs is not { Count: not 0 };

        if (isFullScan)
        {
            if (ctx.Exec.Plan.AllNegated)
            {
                rejectReason = "every filter is a negation (not/!=), so there is no term to drive the scan";
                return false;
            }
            if (ctx.OrderByFields[0].MayHaveMissingEntries)
            {
                rejectReason = "some documents have no value for the sort field (a direct scan can't place them in order)";
                return false;
            }
            if (ctx.OrderByFields[0].FieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
            {
                rejectReason = "the sort field isn't a number or string type";
                return false;
            }
            rejectReason = null;
            return true;
        }

        if (ctx.Exec.SortDrivingClause is null)
        {
            rejectReason = "no equals/range filter on the sort field to drive the scan";
            return false;
        }

        // The driving clause picks which terms tree we walk, and with the sort elided that walk order is the
        // final order - so the ordering type has to match the clause's term type.
        var sortFieldType = ctx.OrderByFields[0].FieldType;
        var drivingTermType = ctx.Exec.SortDrivingClause.TermValueType;
        var orderMatchesScanOrder = drivingTermType switch
        {
            ParamValueType.Long => sortFieldType is MatchCompareFieldType.Integer,
            ParamValueType.Double => sortFieldType is MatchCompareFieldType.Floating,
            ParamValueType.String => sortFieldType is MatchCompareFieldType.Sequence,
            _ => false
        };

        if (orderMatchesScanOrder == false)
        {
            rejectReason = "the ORDER BY type doesn't match the term type of the filter driving the scan";
            return false;
        }

        if (ctx.PlanParams.IndexSearcher.HasMultipleTermsInField(ctx.OrderByFields[0].Field))
        {
            rejectReason = "the sort field holds multiple values per document, so its filter can't be safely skipped during the walk";
            return false;
        }

        if (ctx.Exec.Plan.DirectScanResidualSet is null)
        {
            rejectReason = "a filter can't be checked per-document during the scan";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static bool ResolveNullFirst(in OrderMetadata orderByField, NullsSortMode indexDefault, bool forward)
    {
        bool nullIsSmallest = (orderByField.NullsSortMode ?? indexDefault) == NullsSortMode.NullsSmallest;
        return forward ? nullIsSmallest : nullIsSmallest is false;
    }

    // Compute how many results a direct scan needs to provide. Ideally, we can stoke at Take items, but we may
    // have a filter post query, or need to provide the total result count, etc - that requires more work on our part.
    private static int ResolveSortedScanTake(QueryBuilderParameters builderParams)
    {
        if (HasServerSideFilter(builderParams) || ConsumesExactTotal(builderParams))
            return Constants.IndexSearcher.TakeAll;

        return builderParams?.Take ?? Constants.IndexSearcher.TakeAll;
    }

    // if we need to know the tota AND we have a filter, we *must* read the whole query results
    private static bool CanResolveKnownTotal(QueryBuilderParameters builderParams)
        => ConsumesExactTotal(builderParams) && HasServerSideFilter(builderParams) == false;

    private static bool ConsumesExactTotal(QueryBuilderParameters builderParams)
        => builderParams?.Query is { IsCountQuery: true } or { SkipStatistics: false };

    private static bool HasServerSideFilter(QueryBuilderParameters builderParams)
        => builderParams?.Metadata?.Query?.Filter != null;

    private static long TryCountPostingsInRange(IQueryMatch countMatch, out long probeTicks, out int probeTerms)
    {
        probeTicks = -1; // -1 ticks marks "no probe ran" (the match was not countable).
        probeTerms = 0;
        try
        {
            if (countMatch is not TermsProviderMatch { Provider: IAggregationProvider agg }) 
                return -1;
            
            long t0 = Stopwatch.GetTimestamp();
            var stats = agg.CountPostingsInRange(0);
            probeTicks = Stopwatch.GetTimestamp() - t0;
            probeTerms = stats.Terms;
            return stats.Postings;

        }
        finally
        {
            (countMatch as IDisposable)?.Dispose();
        }
    }

    // if we need to read everything anyway, the cost of doing entry scanning is very high, reflect that
    private static long ResolveEffectiveScanPageSize(QueryBuilderParameters builderParams)
    {
        return ResolveSortedScanTake(builderParams) == Constants.IndexSearcher.TakeAll
            ? long.MaxValue // we have to scan everything, the cost is too high
            : builderParams.Query.PageSize;
    }

    private static string DescribeUnboundedScanTake(QueryBuilderParameters builderParams)
    {
        return builderParams switch
        {
            { Metadata.Query.Filter: not null } => "post-filter present", 
            { Query.IsCountQuery: true } => "count query",
            { Query.SkipStatistics: false } => "statistics requested (SkipStatistics=false, requires count)",
            _ => null
        };
    }

    private static IQueryMatch BuildSortedDrivingWithTieBreakMatch(InstantiateContext ctx, ITermsProvider provider, LowLevelTransaction llt, NullsSortMode indexDefaultNullsSortMode,
        IndexSearcher indexSearcher, bool nullFirst, int take)
    {
        bool secondaryNullIsSmallest = (ctx.OrderByFields[1].NullsSortMode ?? indexDefaultNullsSortMode) == NullsSortMode.NullsSmallest;
        return new SortedDrivingWithTieBreakMatch(provider, llt, ctx.PlanParams.Allocator, indexSearcher,
            ctx.OrderByFields[0].Field, 
            ctx.OrderByFields[1].Field,
            ctx.OrderByFields[1].FieldType, 
            secondaryDescending: ctx.OrderByFields[1].Ascending is false,
            nullFirst: nullFirst, 
            nullIsSmallest: secondaryNullIsSmallest,
            take: take);
    }

    private static (IQueryMatch[], LeafResolveInfo[]) ResolveAllSlots(QueryExecution exec, ResolutionContext walkerCtx, bool planHasBoost)
    {
        Debug.Assert((exec.IsAllEntries && exec.HasSpatialOrVector) is false);

        if (exec.IsAllEntries) // nothing to do here
            return ( [walkerCtx.IndexSearcher.AllEntries()], [new LeafResolveInfo { Kind = LeafResolveKind.PreResolved }]);

        if (exec.Executions is not { Count: > 0 })
            return ([], []);

        var matchList = new List<IQueryMatch>();
        var leafList = new List<LeafResolveInfo>();
        foreach (var clauseExec in exec.Executions)
        {
            ResolveLeafIntoAll(walkerCtx, clauseExec, exec, planHasBoost, matchList, leafList);
        }

        return (matchList.ToArray(), leafList.ToArray());
    }

    private static IQueryMatch ResolveSentinelRewrittenBetween(ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution queryExec, bool forward)
    {
        if (exec.SentinelRewriteType == ClauseType.Exists)
            return indexSearcher.AllEntries();
        if (exec.SentinelRewriteType == ClauseType.LessThanOrEqual)
            return exec.PackedParamValue.RangeQuery(ClauseType.LessThanOrEqual, fieldMeta, indexSearcher, queryExec, forward);

        Debug.Assert(exec.SentinelRewriteType == ClauseType.GreaterThanOrEqual);
        IQueryMatch rangeMatch = exec.PackedParamValue.RangeQuery(ClauseType.GreaterThanOrEqual, fieldMeta, indexSearcher, queryExec, forward);
        if (indexSearcher.TryGetPostingListForNull(in fieldMeta, out _) is false) // no null in this field, we can do a tree scan directly
            return rangeMatch;
        
        // BETWEEN low AND 'NULL' must include null-valued docs (Lucene parity)
        return new LazyOrMatch(indexSearcher.Allocator, rangeMatch, indexSearcher.TermQuery(fieldMeta, null));
    }

    private static IQueryMatch ResolveInTerm(ClauseExecution exec, int termIndex, QueryExecution queryExec, ResolutionContext walkerCtx)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, walkerCtx);
        var termPacked = exec.PackedParamValue.WithTermOffset(termIndex);
        return termPacked.TermQuery(fieldMeta, walkerCtx.IndexSearcher, queryExec);
    }

    internal static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, ResolutionContext walkerCtx)
    {
        var builderParams = walkerCtx.BuilderParams;
        string resolvedFieldName = clause.ResolvedFieldName ?? clause.FieldName;
        bool forceSearchAnalyzer = builderParams.HasDynamics
                                   && !clause.IsExact
                                   && clause.ClauseType != ClauseType.Search
                                   && builderParams.Index.Configuration.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery;
        
        return QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact,
            hasBoost: builderParams.HasBoost, forceDefaultSearchAnalyzer: forceSearchAnalyzer);
    }

    private static bool IsClauseBoosted(ClauseExecution exec) => exec.Clause.HasBoost || exec.BoostFactor > 0;

    private static void EncodeNumericValue(Span<byte> dest, int valueType, int paramIdx, QueryExecution exec, long numericXorMask)
    {
        long raw = valueType == PackedParam.TypeDouble
            ? Bits.DoubleToSortableLong(exec.DoubleValues[paramIdx])
            : exec.LongValues[paramIdx] ^ numericXorMask;
        // CoraxDocumentConverterBase.AppendLong stores `SwapBytes(l ^ mask)` - ensuring the right lexical sort order on the bytes 
        BinaryPrimitives.WriteInt64LittleEndian(dest, Bits.SwapBytes(raw));
    }

    private struct CompoundFieldEncoding
    {
        public PackedParam Packed;
        public Slice Analyzed;
        public int SourceSlot;
        public int Size;
    }

    private static bool TryGetCompoundFieldEncoding(ref InstantiateContext ctx, string fieldName, PackedParam packed, int paramSlot, out CompoundFieldEncoding encoding)
    {
        encoding = default;
        encoding.Packed = packed;
        encoding.SourceSlot = paramSlot;

        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                var meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, fieldName, hasBoost: false);
                encoding.Analyzed = ctx.Exec.GetAnalyzedSlice(ctx.PlanParams.IndexSearcher, meta, paramSlot);
                encoding.Size = encoding.Analyzed.Size;
                return encoding.Size <= byte.MaxValue;
            }
            case PackedParam.TypeLong or PackedParam.TypeDouble:
                encoding.Size = sizeof(long);
                return true;
            default:
                return false;
        }
    }
    
    private static void WriteCompoundFieldEncoding(Span<byte> dest, CompoundFieldEncoding encoding, QueryExecution exec, long numericXorMask)
    {
        if (encoding.Packed.ValueType == PackedParam.TypeString)
        {
            encoding.Analyzed.CopyTo(dest);
            return;
        }
        EncodeNumericValue(dest, encoding.Packed.ValueType, encoding.SourceSlot, exec, numericXorMask);
    }

    internal static bool IsTreeScanEligibleClause(ClauseInfo clause)
    {
        if (clause.HasBoost)
            return false; // need scoring, cannot just scan

        return clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith
            or ClauseType.Exists or ClauseType.Regex
            or ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
            or ClauseType.LessThan or ClauseType.LessThanOrEqual
            or ClauseType.Between;
    }

    internal static MatchDispatch GetDispatch(ClauseExecution exec)
    {
        var clause = exec.Clause;
        if (clause is { HasBoost: false, ClauseType: ClauseType.Equals or ClauseType.NotEquals })
            return MatchDispatch.PostingList;

        if (exec.SentinelRewriteType != null)
            return MatchDispatch.QueryMatch;

        if (IsTreeScanEligibleClause(clause))
            return MatchDispatch.TreeScan;

        return MatchDispatch.QueryMatch;
    }

    private static string FormatValueFromPlan(PackedParam packed, QueryExecution exec, int idx)
    {
        if (idx is PackedParam.NoParamValue)
            return null;
        // An IN clause with all-null terms records InTermCount=0. the packed Param1 still points at the (empty) slot.
        return packed.ValueType switch
        {
            PackedParam.TypeLong => idx < exec.LongValues.Length ? exec.LongValues[idx].ToString() : null,
            PackedParam.TypeDouble => idx < exec.DoubleValues.Length ? exec.DoubleValues[idx].ToString(CultureInfo.InvariantCulture) : null,
            _ => idx < exec.StringValues.Length ? exec.StringValues[idx] : null
        };
    }
}
