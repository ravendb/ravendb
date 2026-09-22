using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Spatial4n.Shapes;
using Constants = Corax.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    /// <summary>Maximum number of ORDER BY fields supported by Corax.</summary>
    private const int MaxSortFields = 16;

    // Check we if we have a seek hint (where Age > $x order by Age) that we can use to speed up the query
    private static void TrySetSortSeekHint(CompiledPlan plan, QueryExecution exec, CompiledQueryMatch match)
    {
        var sortExec = exec.SortSeekClause;
        if (sortExec is null)
            return;

        if (sortExec.PackedParamValue.IsNone)
            return;

        int paramIdx = plan.Template.SortSeekUseParam2 ? sortExec.PackedParamValue.Param2 : sortExec.PackedParamValue.Param1;
        object seekValue = sortExec.PackedParamValue.ValueType switch
        {
            PackedParam.TypeLong => exec.LongValues[paramIdx],
            PackedParam.TypeDouble => exec.DoubleValues[paramIdx],
            PackedParam.TypeString => exec.StringValues[paramIdx],
            _ => null
        };

        if (seekValue != null)
            match.SortHint = new SortHint(sortExec.Clause.FieldName, seekValue);
    }

    // can we use the vector.search() sorted output without a sorting match wrapper? 
    private static bool VectorPostFilterProvidesResultOrder(QueryExecution exec, QueryBuilderParameters bp, OrderMetadata[] orderByFields)
    {
        if (exec.VectorSelects is not { Length: 1 })
            return false; // if we have two vector.search(), we need to sort between them

        if (exec.VectorSelects[0].IsNegated)
            return false; // a negated vector ("not near") has no similarity order to stream

        if (bp.Index is not { HasBoostedFields: false }  || bp.Metadata.HasBoost || bp.IndexSearcher.DocumentsAreBoosted)
            return false; // we have to sort for boosting

        // anything except `order by score()` or `order by score() asc` - we have to sort explicitly
        return orderByFields is [{ FieldType: MatchCompareFieldType.Score, Ascending: true }];
    }

    private static SortMetadataTemplate BuildSortMetadataTemplate(PlanParameters p, PlanTemplate planTemplate)
    {
        // Over the full ORDER BY: which keys drop is decided per execution, so every key needs its metadata built.
        var orderByFields = p.Metadata.OrderBy;
        var elidePins = ComputeSortPins(orderByFields, planTemplate.Clauses, planTemplate.IsOr, p.IndexSearcher);

        if (orderByFields is null)
        {   
            if (p.HasBoost && p.Index is { } indexForScore && // add order by score() if we have boost
                (indexForScore.Configuration.OrderByScoreAutomaticallyWhenBoostingIsInvolved || indexForScore.Configuration.CoraxVectorSearchOrderByScoreAutomatically))
            {
                return new SortMetadataTemplate
                {
                    ImplicitScore = true,
                    HasVectorSearch = p.Metadata.HasVectorSearch,
                    Prebuilt = [new OrderMetadata(true, MatchCompareFieldType.Score)],
                };
            }

            return new SortMetadataTemplate { NoSort = true };
        }

        switch (orderByFields.Length)
        {
            case 0:
                return new SortMetadataTemplate { NoSort = true };
            case > MaxSortFields:
                throw new InvalidOperationException($"Corax does not support ordering by more than {MaxSortFields} properties.");
        }

        var prebuilt = new OrderMetadata[orderByFields.Length];
        var patches = new SortSlotPatch[orderByFields.Length];
        bool anyPatch = false;

        for (int i = 0; i < orderByFields.Length; i++)
        {
            var field = orderByFields[i];

            var orderingType = field.OrderingType;
            switch (field.OrderingType)
            {
                case OrderByFieldType.Random:
                    prebuilt[i] = new OrderMetadata(0);
                    // The seed (literal or parameter) is read from the live query at materialize time, so it is NOT
                    // baked into the cached template and is deliberately absent from the structural key.
                    patches[i].Kind = field.Arguments is { Length: > 0 } ? SortSlotPatchKind.RandomSeeded : SortSlotPatchKind.RandomFreshSeed;
                    patches[i].OrderByIndex = i;
                    anyPatch = true;
                    continue;
                case OrderByFieldType.Score:
                    prebuilt[i] = new OrderMetadata(true, MatchCompareFieldType.Score, field.Ascending);
                    continue;
                case OrderByFieldType.Distance:
                    // The center point/units (literal or parameter) are read from the live query at materialize time.
                    patches[i].Kind = SortSlotPatchKind.DistanceRuntime;
                    patches[i].FieldName = field.Name;
                    patches[i].OrderByIndex = i;
                    anyPatch = true;
                    continue;
                case OrderByFieldType.Implicit when p.Index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && p.Index.IndexFieldsPersistence.HasTimeValues(field.Name.Value):
                    orderingType = OrderByFieldType.Long;
                    break;
            }
            
            var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(p.Allocator, field.Name, p.Index,
                p.HasDynamics, p.DynamicFields, p.IndexFieldsMapping, false);

            prebuilt[i] = new OrderMetadata(fieldMetadata, field.Ascending, 
                GetMatchCompareFieldType(orderingType), GetNullsSortMode(field), 
                mayHaveMissingEntries: fieldMetadata.FieldId == Constants.IndexWriter.DynamicField);
            patches[i].Kind = SortSlotPatchKind.FieldRuntimeResolve;
            patches[i].FieldName = field.Name;
            anyPatch = true;
        }

        return new SortMetadataTemplate
        {
            Prebuilt = prebuilt,
            Patches = anyPatch ? patches : null,
            ElidePins = elidePins,
        };

        MatchCompareFieldType GetMatchCompareFieldType(OrderByFieldType orderingType)
        {
            var compareType = orderingType switch
            {
                OrderByFieldType.Custom => throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy."),
                OrderByFieldType.AlphaNumeric => MatchCompareFieldType.Alphanumeric,
                OrderByFieldType.Long => MatchCompareFieldType.Integer,
                OrderByFieldType.Double => MatchCompareFieldType.Floating,
                _ => MatchCompareFieldType.Sequence,
            };
            return compareType;
        }
    }


    // Per ORDER BY key, the equality that pins it to a single value, null when none. Pinning is structural; whether
    // the key can be dropped needs the bound value's type and is decided per execution.
    private static ClauseInfo[] ComputeSortPins(OrderByField[] orderBy, List<ClauseInfo> clauses, bool isOr, global::Corax.Querying.IndexSearcher indexSearcher)
    {
        if (orderBy is not { Length: > 0 } || isOr)
            return null;

        ClauseInfo[] pins = null;
        for (int i = 0; i < orderBy.Length; i++)
        {
            var field = orderBy[i];
            if (field.OrderingType is OrderByFieldType.Random or OrderByFieldType.Score or OrderByFieldType.Distance ||
                field.Name?.Value is not { } name)
                continue;

            if (indexSearcher.HasMultipleTermsInField(name)) // several terms per document - the equality pins nothing
                continue;

            foreach (var clause in clauses)
            {
                if (clause.ClauseType != ClauseType.Equals || clause.IsNegated || clause.WhenCondition != null ||
                    clause.FieldName is not { } fn || fn != name)
                    continue;

                (pins ??= new ClauseInfo[orderBy.Length])[i] = clause;
                break;
            }
        }

        return pins;
    }

    // Partial sort elision -> WHERE status = 'Released' ORDER BY status, vote DESC → ORDER BY vote DESC.
    // Optimistic: assumes every pin elides. Selects candidate optimizations only, each re-gated per execution.
    private static OrderByField[] ComputeEffectiveOrderBy(OrderByField[] orderBy, List<ClauseInfo> clauses, bool isOr, global::Corax.Querying.IndexSearcher indexSearcher)
    {
        var pins = ComputeSortPins(orderBy, clauses, isOr, indexSearcher);
        if (pins is null)
            return orderBy;

        var kept = new List<OrderByField>(orderBy.Length);
        for (int i = 0; i < orderBy.Length; i++)
        {
            if (pins[i] is null)
                kept.Add(orderBy[i]);
        }

        return kept.ToArray();
    }

    // A value is indexed under several representations (text, long, double) and an equality matches exactly one, so
    // the sort key is pinned only if that is the representation it sorts on. Null excluded: it also matches
    // non-existing entries. Same rule as the driving-scan check in TryCreateSimpleFieldDirectScan.
    private static bool PinReadsSameRepresentation(ParamValueType pinned, MatchCompareFieldType sorted)
    {
        return pinned switch
        {
            ParamValueType.Long => sorted is MatchCompareFieldType.Integer,
            ParamValueType.Double => sorted is MatchCompareFieldType.Floating,
            ParamValueType.String => sorted is MatchCompareFieldType.Sequence or MatchCompareFieldType.Alphanumeric,
            _ => false
        };
    }

    private static NullsSortMode? GetNullsSortMode(OrderByField field)
    {
        var nullsSortMode = (field.NullsOrdering, field.Ascending) switch
        {
            (NullsOrderingType.First, Ascending: true) => NullsSortMode.NullsSmallest,
            (NullsOrderingType.First, Ascending: false) => NullsSortMode.NullsLargest,
            (NullsOrderingType.Last, Ascending: true) => NullsSortMode.NullsLargest,
            (NullsOrderingType.Last, Ascending: false) => NullsSortMode.NullsSmallest,
            _ => (NullsSortMode?)null
        };
        return nullsSortMode;
    }

    private static OrderMetadata[] GetSortMetadata(QueryBuilderParameters builderParameters, PlanTemplate planTemplate, QueryExecution exec)
    {
        // PageSize == 0 (count-only) is per-query and short-circuits all the sort work.
        if (builderParameters.Query.PageSize == 0)
            return null;

        return MaterializeSortMetadata(planTemplate.SortMetadataTemplate, builderParameters, exec);
    }

    private static int CountPins(ClauseInfo[] pins)
    {
        int count = 0;
        foreach (var pin in pins)
        {
            if (pin is not null)
                count++;
        }

        return count;
    }

    /// <summary>The sort keys this execution drops: a pinned key whose equality matched the representation it sorts on.</summary>
    private static bool[] ResolveElidedSortSlots(SortMetadataTemplate template, QueryExecution exec, out int elided)
    {
        elided = 0;
        if (template.ElidePins is null)
            return null;

        bool[] drop = null;
        for (int i = 0; i < template.ElidePins.Length; i++)
        {
            if (template.ElidePins[i] is not { } pin)
                continue;

            foreach (var clause in exec.Executions)
            {
                if (ReferenceEquals(clause.Clause, pin) == false)
                    continue;

                // A sentinel has no populated value, so its type says nothing.
                if (clause.IsSentinel == false && PinReadsSameRepresentation(clause.TermValueType, template.Prebuilt[i].FieldType))
                {
                    (drop ??= new bool[template.ElidePins.Length])[i] = true;
                    elided++;
                }

                break;
            }
        }

        return drop;
    }

    /// <summary>Runtime materializer: apply per-query patches to the template's prebuilt array.</summary>
    private static OrderMetadata[] MaterializeSortMetadata(SortMetadataTemplate template, QueryBuilderParameters builderParameters, QueryExecution exec)
    {
        if (template.NoSort)
            return null;

        if (template.ImplicitScore)
        {
            builderParameters.IndexReadOperation.AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved(template.HasVectorSearch);
            return template.Prebuilt;
        }

        var drop = ResolveElidedSortSlots(template, exec, out var elided);

        // Sorted scans were picked from the elided ORDER BY; a surviving key invalidates that choice.
        exec.SortElisionDivergedFromTemplate = template.ElidePins is not null && elided != CountPins(template.ElidePins);

        if (elided == template.Prebuilt.Length)
            return null;

        if (template.Patches is null && drop is null) // hot path
            return template.Prebuilt;

        var indexSearcher = builderParameters.IndexSearcher;

        var result = new OrderMetadata[template.Prebuilt.Length - elided];

        for (int i = 0, slot = 0; i < template.Prebuilt.Length; i++)
        {
            if (drop is not null && drop[i])
                continue;

            int target = slot++;
            if (template.Patches is null)
            {
                result[target] = template.Prebuilt[i];
                continue;
            }

            ref var patch = ref template.Patches[i];
            switch (patch.Kind)
            {
                case SortSlotPatchKind.None:
                    result[target] = template.Prebuilt[i];
                    break;

                case SortSlotPatchKind.RandomFreshSeed:
                    result[target] = new OrderMetadata(Random.Shared.Next());
                    break;

                case SortSlotPatchKind.RandomSeeded:
                {
                    // Seed (literal or parameter) read from the live ORDER BY arguments, so plans with different
                    // seeds share one cached template/structural bucket.
                    var seedArg = builderParameters.Metadata.OrderBy[patch.OrderByIndex].Arguments[0];
                    var seedValue = seedArg.GetString(builderParameters.Query.QueryParameters);
                    var seed = (int)Hashing.XXHash32.CalculateRaw(seedValue ?? string.Empty);
                    result[target] = new OrderMetadata(seed);
                    break;
                }

                case SortSlotPatchKind.FieldRuntimeResolve:
                {
                    // FieldMetadata holds transaction-bound slices, so re-resolve it per query
                    var fieldMeta = ResolveSortFieldMeta(builderParameters, patch.FieldName);
                    var p = template.Prebuilt[i];
                    bool mayHaveMissingEntries = p.MayHaveMissingEntries
                        || indexSearcher.GetDistinctTermCountInField(fieldMeta) == 0 // no entries at all
                        || indexSearcher.HasAnyNonExistingEntries(fieldMeta);         
                    result[target] = new OrderMetadata(fieldMeta, p.Ascending, p.FieldType, p.NullsSortMode, mayHaveMissingEntries);
                    break;
                }

                case SortSlotPatchKind.DistanceRuntime:
                {
                    // Center point/units read from the live ORDER BY arguments, so plans with different
                    // coordinates share one cached template/structural bucket.
                    var liveField = builderParameters.Metadata.OrderBy[patch.OrderByIndex];
                    var fieldMeta = ResolveSortFieldMeta(builderParameters, patch.FieldName);
                    result[target] = BuildDistanceOrderMetadata(builderParameters, liveField, fieldMeta);
                    break;
                }
            }
        }

        return result;
    }

    private static FieldMetadata ResolveSortFieldMeta(QueryBuilderParameters builderParameters, string fieldName)
    {
        // FieldMetadata hold slices, which are tied to the current transaction, so have to do this per query 
        return QueryBuilderHelper.GetFieldIdForOrderBy(builderParameters.Allocator, fieldName, builderParameters.Index,
            builderParameters.HasDynamics, builderParameters.DynamicFields, builderParameters.IndexFieldsMapping, false);
    }

    private static OrderMetadata BuildDistanceOrderMetadata(QueryBuilderParameters builderParameters,
        OrderByField field, FieldMetadata fieldMetadata)
    {
        var query = builderParameters.Query;
        var getSpatialField = builderParameters.Factories.GetSpatialFieldFactory;
        var spatialField = getSpatialField(field.Name);

        int lastArgument;
        IPoint point;
        switch (field.Method)
        {
            case MethodType.Spatial_Circle:
                var cLatitude = field.Arguments[1].GetDouble(query.QueryParameters);
                var cLongitude = field.Arguments[2].GetDouble(query.QueryParameters);
                lastArgument = 2;
                point = spatialField.ReadPoint(cLatitude, cLongitude).Center;
                break;
            case MethodType.Spatial_Wkt:
                var wkt = field.Arguments[0].GetString(query.QueryParameters);
                SpatialUnits? spatialUnits = null;
                lastArgument = 1;
                if (field.Arguments.Length > 1)
                {
                    spatialUnits = Enum.Parse<SpatialUnits>(field.Arguments[1].GetString(query.QueryParameters), ignoreCase: true);
                    lastArgument = 2;
                }
                point = spatialField.ReadShape(wkt, spatialUnits).Center;
                break;
            case MethodType.Spatial_Point:
                var pLatitude = field.Arguments[0].GetDouble(query.QueryParameters);
                var pLongitude = field.Arguments[1].GetDouble(query.QueryParameters);
                lastArgument = 2;
                point = spatialField.ReadPoint(pLatitude, pLongitude).Center;
                break;
            default:
                throw new ArgumentOutOfRangeException(field.Method.ToString());
        }

        var roundTo = field.Arguments.Length > lastArgument
            ? field.Arguments[lastArgument].GetDouble(query.QueryParameters)
            : 0D;

        return new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Spatial, point, roundTo,
            spatialField.Units is SpatialUnits.Kilometers
                ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                : global::Corax.Utils.Spatial.SpatialUnits.Miles, GetNullsSortMode(field));
    }

    private static IQueryMatch OrderBy(QueryBuilderParameters builderParameters, IQueryMatch match, in OrderMetadata[] orderMetadata)
    {
        var indexSearcher = builderParameters.IndexSearcher;
        var take = builderParameters.Take;

        switch (orderMetadata.Length)
        {
            case 0:
                return match;
            case 1:
                return indexSearcher.OrderBy(match, orderMetadata[0], builderParameters.Index.Configuration.NullsSortMode, take, builderParameters.Token);
            default:
                return indexSearcher.OrderBy(match, orderMetadata, builderParameters.Index.Configuration.NullsSortMode, take, builderParameters.Token);
        }
    }


    private static FieldMetadata ReplaceAnalyzerForWildcardQueries(FieldMetadata searchMeta, ResolutionContext walkerCtx)
    {
        var result = searchMeta;
        var indexFieldsMapping = walkerCtx.BuilderParams.IndexFieldsMapping;

        if (searchMeta.IsDynamic && indexFieldsMapping != null)
            result = searchMeta.ChangeAnalyzer(searchMeta.Mode, indexFieldsMapping.SearchAnalyzer(searchMeta.FieldName.ToString()));

        if (result.Analyzer is Lucene.LuceneAnalyzerAdapter laa && indexFieldsMapping != null)
        {
            global::Corax.Analyzers.Analyzer replacementAnalyzer = laa.Analyzer switch
            {
                global::Lucene.Net.Analysis.KeywordAnalyzer => indexFieldsMapping.ExactAnalyzer(searchMeta.FieldName.ToString()),
                Lucene.Analyzers.RavenStandardAnalyzer
                    or Lucene.Analyzers.NGramAnalyzer => indexFieldsMapping.DefaultAnalyzer,
                global::Lucene.Net.Analysis.Standard.StandardAnalyzer when laa.Analyzer.GetType() == typeof(global::Lucene.Net.Analysis.Standard.StandardAnalyzer)
                    => indexFieldsMapping.DefaultAnalyzer,
                Lucene.Analyzers.LowerCaseKeywordAnalyzer
                    or Lucene.Analyzers.Collation.CollationAnalyzer => indexFieldsMapping.DefaultAnalyzer,
                _ => null
            };

            if (replacementAnalyzer != null)
                result = result.ChangeAnalyzer(global::Corax.FieldIndexingMode.Search, replacementAnalyzer);
        }

        return result;
    }
}
