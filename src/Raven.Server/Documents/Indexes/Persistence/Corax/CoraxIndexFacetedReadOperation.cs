using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Corax;
using Corax.Mappings;
using IQueryMatch = Corax.Querying.Matches.Meta.IQueryMatch;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Voron;
using Voron.Impl;
using IndexSearcher = Corax.Querying.IndexSearcher;
using RangeType = Raven.Client.Documents.Indexes.RangeType;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class CoraxIndexFacetedReadOperation : IndexFacetReadOperationBase
{
    private readonly IndexFieldsMapping _fieldMappings;
    private readonly Dictionary<string, long> _fieldNameToRootPage = new();
    private readonly IndexSearcher _indexSearcher;
    private readonly ByteStringContext _allocator;

    public CoraxIndexFacetedReadOperation(Index index, RavenLogger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping) : base(index, queryBuilderFactories, logger)
    {
        _fieldMappings = fieldsMapping;
        _allocator = readTransaction.Allocator;
        _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings)
        {
            MaxFacetQueryFilterSizeInBytes = index.Configuration.MaxFacetQueryFilterSize.GetValue(SizeUnit.Bytes),
            PlanCache = (index.IndexPersistence as CoraxIndexPersistence)?.SharedPlanCache ?? new global::Corax.Querying.Planning.PlanCache(),
        };
    }

    public override List<FacetResult> FacetedQuery(FacetQuery facetQuery, QueryTimingsScope queryTimings, DocumentsOperationContext context,
        Func<string, SpatialField> getSpatialField, QueryTimeScope queryTime, CancellationToken token)
    {
        // Use the indexed path (posting-list operations) when possible.
        // The scanning path is only needed for aggregations (sum/avg/min/max) and AllResults facets
        // which require per-document field access via EntryTermsReader.
        // When a WHERE clause is present, the indexed path materializes matching doc IDs once
        // and intersects with each facet's posting list.
        var canUseIndexedFacetQuery = true;
        var results = FacetedQueryParser.Parse(context, facetQuery, SearchEngineType.Corax);
        foreach (var result in results)
        {
            canUseIndexedFacetQuery &= result.Value.Aggregations?.Count == 0;
            canUseIndexedFacetQuery &= result.Key != Client.Constants.Documents.Querying.Facet.AllResults;
            canUseIndexedFacetQuery &= result.Value.AggregateBy != Client.Constants.Documents.Querying.Facet.AllResults;
        }

        return canUseIndexedFacetQuery
            ? IndexedFacetedQuery(results, facetQuery, queryTimings, queryTime, token)
            : ScanningFacetedQuery(results, facetQuery, queryTimings, queryTime, token);
    }

    private List<FacetResult> IndexedFacetedQuery(Dictionary<string, FacetedQueryParser.FacetResult> results, FacetQuery facetQuery, QueryTimingsScope queryTimings, QueryTimeScope queryTime, CancellationToken token)
    {
        var query = facetQuery.Query;
        Dictionary<string, Dictionary<string, FacetValues>> facetsByName = new();
        Dictionary<string, Dictionary<string, FacetValues>> facetsByRange = new();
        var coraxPageSize = CoraxBufferSize(_indexSearcher, facetQuery.Query.PageSize, query);
        var ids = CoraxIndexReadOperation.QueryPool.Rent(coraxPageSize);
        CreateMappingForRanges(results, facetsByRange, facetQuery);

        // When a WHERE clause is present, materialize all matching doc IDs into a HashSet.
        // Both term and range facets intersect their posting lists against this set.
        HashSet<long> baseQueryMatchingIds = null;
        global::Corax.Querying.Matches.Meta.IBitmapQueryMatch baseQueryBitmap = null;
        // baseQueryDisposable owns the bitmap allocations of a CompiledQueryMatch base query;
        // disposed at the end of this method (or before the scanning fallback runs).
        IDisposable baseQueryDisposable = null;
        if (query.Metadata.Query.Where is not null)
        {
            var parameters = new QueryBuilderParameters(_indexSearcher, _allocator, null, null, query, _index,
                query.QueryParameters, _queryBuilderFactories, _fieldMappings, null, null, global::Corax.Constants.IndexSearcher.TakeAll,
                token: token);

            IQueryMatch baseQuery = QueryPlanBuilder.QueryPlanBuilder.BuildFilterMatch(
                new QueryPlanBuilder.PlanParameters
                {
                    IndexSearcher = _indexSearcher,
                    Metadata = query.Metadata,
                    QueryParameters = query.QueryParameters,
                    Index = _index,
                    IndexFieldsMapping = _fieldMappings,
                    Allocator = _allocator,
                    HasDynamics = parameters.HasDynamics,
                    DynamicFields = parameters.DynamicFields,
                    HasBoost = parameters.HasBoost
                }, parameters, highlightingTerms: null, wantTimings: false, token);
            baseQueryDisposable = baseQuery as IDisposable;
            queryTimings?.SetQueryPlan(baseQuery.Inspect());

            // If the base query is bitmap-backed, use it directly for Contains checks
            // instead of materializing into a HashSet.
            if (baseQuery is global::Corax.Querying.Matches.Meta.IBitmapQueryMatch bqBitmap)
            {
                // Force execution so bitmap is populated
                _ = bqBitmap.Count;
                baseQueryBitmap = bqBitmap;
            }
            else
            {
                var maxMatchingIds = _indexSearcher.MaxFacetQueryFilterSizeInBytes / sizeof(long);
                baseQueryMatchingIds = new HashSet<long>();
                int read;
                while ((read = baseQuery.Fill(ids)) != 0)
                {
                    for (int i = 0; i < read; i++)
                        baseQueryMatchingIds.Add(ids[i]);

                    token.ThrowIfCancellationRequested();

                    // When exceeded, fall back to the scanning path which streams with bounded memory.
                    if (baseQueryMatchingIds.Count > maxMatchingIds)
                    {
                        baseQueryDisposable?.Dispose();
                        CoraxIndexReadOperation.QueryPool.Return(ids);
                        return ScanningFacetedQuery(results, facetQuery, queryTimings, queryTime, token);
                    }
                }
            } // end else (non-bitmap path)
        }

        foreach (var result in results)
        {
            using var facetTiming = queryTimings?.For($"{nameof(QueryTimingsScope.Names.AggregateBy)}/{result.Key}");
            Dictionary<string, FacetValues> facetValues;
            if (result.Value.Ranges == null || result.Value.Ranges.Count == 0)
            {
                if (facetsByName.TryGetValue(result.Key, out facetValues) == false)
                    facetsByName[result.Key] = facetValues = new Dictionary<string, FacetValues>();

                var metadata = GetFieldMetadata(result.Key);

                var provider = _indexSearcher.TextualAggregation(metadata, forward: result.Value.Options.TermSortMode is not FacetTermSortMode.ValueDesc);
                // When WHERE is present, we must NOT set SortedIds: UpdateFacetResults uses it as the
                // authoritative term list and would emit FacetValue{count=0} for every term in it,
                // including terms filtered out by the WHERE clause.
                using var aggregationScope = provider.AggregateByTerms(out List<string> sortedIds, out var counts);
                if (baseQueryMatchingIds == null && baseQueryBitmap == null)
                    result.Value.SortedIds = sortedIds;

                var idX = 0;
                foreach (var term in CollectionsMarshal.AsSpan(sortedIds))
                {
                    long count;
                    if (baseQueryMatchingIds != null || baseQueryBitmap != null)
                    {
                        var queryTerm = ReferenceEquals(term, Constants.ProjectionNullValue) ? null
                            : ReferenceEquals(term, Constants.ProjectionEmptyString) ? Constants.EmptyString
                            : term;
                        var termMatch = _indexSearcher.TermQuery(metadata, queryTerm);
                        count = 0;
                        int read;
                        while ((read = termMatch.Fill(ids)) != 0)
                        {
                            // ids is sorted+deduped per the Fill contract, so when the base query is a bitmap we can
                            // count membership with one grouped merge instead of a point lookup per id.
                            if (baseQueryBitmap != null)
                            {
                                count += baseQueryBitmap.BitmapState.CountPresentSorted(ids.AsSpan(0, read));
                            }
                            else
                            {
                                for (int i = 0; i < read; i++)
                                {
                                    if (baseQueryMatchingIds.Contains(ids[i]))
                                        count++;
                                }
                            }
                        }
                    }
                    else
                    {
                        count = counts[idX];
                    }

                    idX++;

                    if (count == 0)
                        continue;

                    ref var collectionOfFacetValues = ref CollectionsMarshal.GetValueRefOrAddDefault(facetValues, term, out var exists);
                    if (exists == false)
                    {
                        var range = FacetedQueryHelper.GetRangeName(result.Value.AggregateBy, term);
                        collectionOfFacetValues = new FacetValues(facetQuery.Legacy);
                        collectionOfFacetValues.AddDefault(range);
                    }

                    collectionOfFacetValues.IncrementCount((int)count);
                }
            }


            if (facetsByRange.TryGetValue(result.Key, out facetValues) == false)
            {
                facetValues = new();
                facetsByRange.Add(result.Key, facetValues);
            }

            var ranges = result.Value.Ranges;
            foreach (var parsedRange in ranges ?? Enumerable.Empty<FacetedQueryParser.ParsedRange>())
            {
                if (parsedRange is not FacetedQueryParser.CoraxParsedRange range)
                    continue;

                ref var collectionOfFacetValues = ref CollectionsMarshal.GetValueRefOrAddDefault(facetValues, parsedRange.RangeText, out var exists);
                if (exists == false)
                    collectionOfFacetValues = new FacetValues(facetQuery.Legacy);

                var fieldMetadata = GetFieldMetadata(range.Field);
                long count;

                if (baseQueryMatchingIds != null || baseQueryBitmap != null)
                {
                    var rangeQuery = range.GetQuery(_indexSearcher, fieldMetadata);
                    count = 0;
                    int read;
                    while ((read = rangeQuery.Fill(ids)) != 0)
                    {
                        // ids is sorted+deduped per the Fill contract; batch the bitmap membership count.
                        if (baseQueryBitmap != null)
                        {
                            count += baseQueryBitmap.BitmapState.CountPresentSorted(ids.AsSpan(0, read));
                        }
                        else
                        {
                            for (int i = 0; i < read; i++)
                            {
                                if (baseQueryMatchingIds.Contains(ids[i]))
                                    count++;
                            }
                        }
                    }
                }
                else
                {
                    var aggregationProvider = range.GetAggregation(_indexSearcher, fieldMetadata, true);
                    count = aggregationProvider.AggregateByRange();
                }

                collectionOfFacetValues.IncrementCount((int)count);

                token.ThrowIfCancellationRequested();
            }
        }


        UpdateRangeResults(results, facetsByRange);
        UpdateFacetResults(results, query, facetsByName);
        CompleteFacetCalculationsStage(results, query);

        baseQueryDisposable?.Dispose();
        CoraxIndexReadOperation.QueryPool.Return(ids);
        return results.Values
            .Select(x => x.Result)
            .ToList();


        FieldMetadata GetFieldMetadata(string name) => QueryBuilderHelper.GetFieldMetadata(_allocator, name, _index, _fieldMappings,
            _index.Definition.HasDynamicFields,
            _index.Definition.HasDynamicFields ? new Lazy<List<string>>(() => _indexSearcher.GetFields()) : null, exact: true, hasBoost: true);
    }

    private List<FacetResult> ScanningFacetedQuery(Dictionary<string, FacetedQueryParser.FacetResult> results, FacetQuery facetQuery, QueryTimingsScope queryTimings, QueryTimeScope queryTime, CancellationToken token)
    {
        var query = facetQuery.Query;
        Dictionary<string, Dictionary<string, FacetValues>> facetsByName = new();
        Dictionary<string, Dictionary<string, FacetValues>> facetsByRange = new();

        var parameters = new QueryBuilderParameters(_indexSearcher, _allocator, null, null, query, _index, query.QueryParameters, _queryBuilderFactories,
            _fieldMappings, null, null, global::Corax.Constants.IndexSearcher.TakeAll, token: token, queryTime: queryTime);

        IQueryMatch baseQuery = query.Metadata.Query.Where == null
            ? _indexSearcher.AllEntries()
            : QueryPlanBuilder.QueryPlanBuilder.BuildFilterMatch(
                new QueryPlanBuilder.PlanParameters
                {
                    IndexSearcher = _indexSearcher,
                    Metadata = query.Metadata,
                    QueryParameters = query.QueryParameters,
                    Index = _index,
                    IndexFieldsMapping = _fieldMappings,
                    Allocator = _allocator,
                    HasDynamics = parameters.HasDynamics,
                    DynamicFields = parameters.DynamicFields,
                    HasBoost = parameters.HasBoost
                }, parameters, highlightingTerms: null, wantTimings: false, token);

        var coraxPageSize = CoraxBufferSize(_indexSearcher, facetQuery.Query.PageSize, query);
        var ids = CoraxIndexReadOperation.QueryPool.Rent(coraxPageSize);

        Page page = default;
        CreateMappingForRanges(results, facetsByRange, facetQuery);

        try
        {
            int read;
            while ((read = baseQuery.Fill(ids)) != 0)
            {
                for (int docId = 0; docId < read; docId++)
                {
                    var reader = _indexSearcher.GetEntryTermsReader(ids[docId], ref page);
                    foreach (var result in results)
                    {
                        token.ThrowIfCancellationRequested();

                        using var facetTiming = queryTimings?.For($"{nameof(QueryTimingsScope.Names.AggregateBy)}/{result.Key}");

                        if (result.Value.Ranges == null || result.Value.Ranges.Count == 0)
                        {
                            HandleFacetsPerDocument(ref reader, result, facetsByName, facetQuery.Legacy, token);
                            continue;
                        }

                        // Cache facetByRange because we will fulfill data in batches instead of whole collection
                        if (facetsByRange.TryGetValue(result.Key, out var facetValues) == false)
                        {
                            facetValues = new();
                            facetsByRange.Add(result.Key, facetValues);
                        }

                        HandleRangeFacetsPerDocument(ref reader, result.Value, facetValues, token);
                    }
                }

                token.ThrowIfCancellationRequested();
            }

            UpdateRangeResults(results, facetsByRange);

            UpdateFacetResults(results, query, facetsByName);

            CompleteFacetCalculationsStage(results, query);
            queryTimings?.SetQueryPlan(baseQuery.Inspect());

            return results.Values
                .Select(x => x.Result)
                .ToList();
        }
        finally
        {
            (baseQuery as IDisposable)?.Dispose();
            CoraxIndexReadOperation.QueryPool.Return(ids);
        }
    }

    private void UpdateRangeResults(Dictionary<string, FacetedQueryParser.FacetResult> results, Dictionary<string, Dictionary<string, FacetValues>> facetsByRange)
    {
        foreach (var result in results)
        {
            foreach (var kvp in facetsByRange)
            {
                if (result.Key == kvp.Key)
                {
                    foreach (var inner in kvp.Value)
                    {
                        if (inner.Value.Any == false)
                        {
                            continue;
                        }

                        result.Value.Result.Values.AddRange(inner.Value.GetAll());
                    }
                }
            }
        }
    }

    private void CreateMappingForRanges(Dictionary<string, FacetedQueryParser.FacetResult> results, Dictionary<string, Dictionary<string, FacetValues>> facetsByRange,
        FacetQuery facetQuery)
    {
        foreach (var result in results)
        {
            if (result.Value.Ranges == null || result.Value.Ranges.Count == 0)
                continue;

            // Cache facetByRange because we will fulfill data in batches instead of whole collection
            if (facetsByRange.TryGetValue(result.Key, out var facetValues) == false)
            {
                facetValues = new();
                facetsByRange.Add(result.Key, facetValues);
            }

            foreach (var range in result.Value.Ranges)
            {
                var key = range.RangeText;
                if (facetValues.TryGetValue(key, out var collectionOfFacetValues))
                    continue;

                collectionOfFacetValues = new FacetValues(facetQuery.Legacy);
                if (result.Value.Aggregations.Count <= 0)
                {
                    collectionOfFacetValues.AddDefault(key);
                }
                else
                {
                    foreach (var aggregation in result.Value.Aggregations)
                        collectionOfFacetValues.Add(aggregation.Key, key);
                }

                facetValues.Add(key, collectionOfFacetValues);
            }
        }
    }

    private void HandleRangeFacetsPerDocument(ref EntryTermsReader reader, FacetedQueryParser.FacetResult result,
        Dictionary<string, FacetValues> facetValues,
        CancellationToken token)
    {
        var needToApplyAggregation = result.Aggregations.Count > 0;
        var ranges = result.Ranges;
        if (ranges == null || ranges.Count == 0)
            return;

        // Read the field value once per document, then check every range against it.
        var firstRange = ranges[0] as FacetedQueryParser.CoraxParsedRange;
        if (firstRange == null)
            return;

        var fieldRootPage = GetFieldRootPage(firstRange.Field);
        reader.Reset();
        bool fieldFound = false;
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull || reader.IsNonExisting)
                continue; // skip null/non-existing entries, look for actual value
            fieldFound = true;
            break;
        }
        if (!fieldFound)
            return;

        var currentDouble = reader.CurrentDouble;
        var currentLong = reader.CurrentLong;
        byte[] currentDecodedBytes = result.RangeType == RangeType.None ? reader.Current.Decoded().ToArray() : null;

        foreach (var parsedRange in ranges)
        {
            if (parsedRange is not FacetedQueryParser.CoraxParsedRange range)
                continue;

            bool isMatching = result.RangeType switch
            {
                RangeType.Double => range.IsMatch(currentDouble),
                RangeType.Long => range.IsMatch(currentLong),
                _ => range.IsMatch(currentDecodedBytes.AsSpan())
            };

            var collectionOfFacetValues = facetValues[range.RangeText];
            if (isMatching)
            {
                collectionOfFacetValues.IncrementCount(1);
                if (needToApplyAggregation)
                    ApplyAggregation(result.Aggregations, collectionOfFacetValues, ref reader);
            }
            token.ThrowIfCancellationRequested();
        }
    }

    private void HandleFacetsPerDocument(ref EntryTermsReader reader,
        KeyValuePair<string, FacetedQueryParser.FacetResult> result,
        Dictionary<string, Dictionary<string, FacetValues>> facetsByName,
        bool legacy,
        CancellationToken token)
    {
        var needToApplyAggregation = result.Value.Aggregations.Count > 0;
        if (facetsByName.TryGetValue(result.Key, out var facetValues) == false)
            facetsByName[result.Key] = facetValues = new Dictionary<string, FacetValues>();

        if (result.Key == Client.Constants.Documents.Querying.Facet.AllResults || result.Value.AggregateBy == Client.Constants.Documents.Querying.Facet.AllResults)
        {
            InsertTerm(Encodings.Utf8.GetBytes(result.Value.AggregateBy), ref reader, facetValues, result, legacy, needToApplyAggregation, token);
            return;
        }

        long fieldRootPage = GetFieldRootPage(result.Value.AggregateBy);

        var cloned = reader;
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNonExisting)
                continue;

            var key = reader.IsNull
                ? Constants.ProjectionNullValueSlice
                : reader.Current.Decoded();

            if (key.SequenceEqual(Constants.EmptyStringByteSpan))
                key = Constants.ProjectionEmptyStringSlice;

            InsertTerm(key, ref cloned, facetValues, result, legacy, needToApplyAggregation, token);
        }
    }

    private void InsertTerm(ReadOnlySpan<byte> term, ref EntryTermsReader reader, Dictionary<string, FacetValues> facetValues,
        KeyValuePair<string, FacetedQueryParser.FacetResult> result, bool legacy, bool needToApplyAggregation, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        var encodedTerm = Encodings.Utf8.GetString(term);

        if (facetValues.TryGetValue(encodedTerm, out var collectionOfFacetValues) == false)
        {
            var range = FacetedQueryHelper.GetRangeName(result.Value.AggregateBy, encodedTerm);
            collectionOfFacetValues = new FacetValues(legacy);
            if (needToApplyAggregation == false)
                collectionOfFacetValues.AddDefault(range);
            else
            {
                foreach (var aggregation in result.Value.Aggregations)
                    collectionOfFacetValues.Add(aggregation.Key, range);
            }

            facetValues.Add(encodedTerm, collectionOfFacetValues);
        }

        collectionOfFacetValues.IncrementCount(1);

        if (needToApplyAggregation)
        {
            ApplyAggregation(result.Value.Aggregations, collectionOfFacetValues, ref reader);
        }
    }

    private long GetFieldRootPage(string fieldName)
    {
        ref var fieldRootPage = ref CollectionsMarshal.GetValueRefOrAddDefault(_fieldNameToRootPage, fieldName, out var exists);
        if (exists == false)
        {
            fieldRootPage = _indexSearcher.FieldCache.GetLookupRootPage(fieldName);
        }

        return fieldRootPage;
    }

    private void ApplyAggregation(Dictionary<FacetAggregationField, FacetedQueryParser.FacetResult.Aggregation> aggregations, FacetValues values,
        ref EntryTermsReader reader)
    {
        foreach (var kvp in aggregations)
        {
            if (string.IsNullOrEmpty(kvp.Key.Name)) // Count
                continue;

            var value = values.Get(kvp.Key);

            var name = kvp.Key.Name;
            var val = kvp.Value;
            double min = value.Min ?? double.MaxValue, max = value.Max ?? double.MinValue, sum = value.Sum ?? 0, avg = value.Average ?? 0;

            var fieldRootPage = GetFieldRootPage(name);

            reader.Reset();
            while (reader.FindNext(fieldRootPage))
            {
                sum += reader.CurrentDouble;
                avg += reader.CurrentDouble;
                min = Math.Min(min, reader.CurrentDouble);
                max = Math.Max(max, reader.CurrentDouble);
            }

            if (val.Min)
            {
                value.Min = min;
            }

            if (val.Average)
            {
                value.Average = avg;
            }

            if (val.Max)
            {
                value.Max = max;
            }

            if (val.Sum)
            {
                value.Sum = sum;
            }
        }
    }
}
