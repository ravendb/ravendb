using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server;
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
    private readonly Dictionary<string, Slice> _fieldNameCache;

    public CoraxIndexFacetedReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping) : base(index, queryBuilderFactories, logger)
    {
        _fieldMappings = fieldsMapping;
        _allocator = readTransaction.Allocator;
        _fieldMappings = fieldsMapping;
        _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings)
        {
            MaxMemoizationSizeInBytes = index.Configuration.MaxMemoizationSize.GetValue(SizeUnit.Bytes)
        };
        _fieldNameCache = new();
    }

    public override List<FacetResult> FacetedQuery(FacetQuery facetQuery, QueryTimingsScope queryTimings, DocumentsOperationContext context,
        Func<string, SpatialField> getSpatialField, CancellationToken token)
    {
        //We currently only supports count aggregation by index, however in scanning we already have entry reader so let's use indexed facet only in case when user wants to count per term/range
        var canUseIndexedFacetQuery = true;
        canUseIndexedFacetQuery &= facetQuery.Query.Metadata.Query.Where is null;

        var results = FacetedQueryParser.Parse(context, facetQuery, SearchEngineType.Corax);
        foreach (var result in results)
        {
            canUseIndexedFacetQuery &= result.Value.Aggregations?.Count == 0;
            canUseIndexedFacetQuery &= result.Key != Client.Constants.Documents.Querying.Facet.AllResults;
            canUseIndexedFacetQuery &= result.Value.AggregateBy != Client.Constants.Documents.Querying.Facet.AllResults;
        }

        return canUseIndexedFacetQuery
            ? IndexedFacetedQuery(results, facetQuery, queryTimings, context, getSpatialField, token)
            : ScanningFacetedQuery(results, facetQuery, queryTimings, context, getSpatialField, token);
    }

    private List<FacetResult> IndexedFacetedQuery(Dictionary<string, FacetedQueryParser.FacetResult> results, FacetQuery facetQuery, QueryTimingsScope queryTimings,
        DocumentsOperationContext context, Func<string, SpatialField> getSpatialField, CancellationToken token)
    {
        var query = facetQuery.Query;
        Dictionary<string, Dictionary<string, FacetValues>> facetsByName = new();
        Dictionary<string, Dictionary<string, FacetValues>> facetsByRange = new();
        var coraxPageSize = CoraxBufferSize(_indexSearcher, facetQuery.Query.PageSize, query);
        var ids = CoraxIndexReadOperation.QueryPool.Rent(coraxPageSize);
        CreateMappingForRanges(results, facetsByRange, facetQuery);
        foreach (var result in results)
        {
            var needToApplyAggregation = result.Value.Aggregations.Count > 0;
            Dictionary<string, FacetValues> facetValues;
            if (result.Value.Ranges == null || result.Value.Ranges.Count == 0)
            {
                if (facetsByName.TryGetValue(result.Key, out facetValues) == false)
                    facetsByName[result.Key] = facetValues = new Dictionary<string, FacetValues>();

                var metadata = GetFieldMetadata(result.Key);

                var provider = _indexSearcher.TextualAggregation(metadata, forward: result.Value.Options.TermSortMode is not FacetTermSortMode.ValueDesc);
                using var _ = provider.AggregateByTerms(out result.Value.SortedIds, out var counts);

                var idX = 0;
                foreach (var term in CollectionsMarshal.AsSpan(result.Value.SortedIds))
                {
                    ref var collectionOfFacetValues = ref CollectionsMarshal.GetValueRefOrAddDefault(facetValues, term, out var exists);
                    if (exists == false)
                    {
                        var range = FacetedQueryHelper.GetRangeName(result.Value.AggregateBy, term);
                        collectionOfFacetValues = new FacetValues(facetQuery.Legacy);
                        if (needToApplyAggregation == false)
                            collectionOfFacetValues.AddDefault(range);
                        else
                        {
                            foreach (var aggregation in result.Value.Aggregations)
                                collectionOfFacetValues.Add(aggregation.Key, range);
                        }
                    }

                    collectionOfFacetValues.IncrementCount((int)counts[idX++]);
                    if (needToApplyAggregation)
                    {
                        //not supported yet.
                        throw new InvalidOperationException("Facet queries that need to apply aggregation should be handled via scanning reader. This code path is not supposed to be reached for such queries.");
                    }

                    continue;
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
                var aggregationProvider = range.GetAggregation(_indexSearcher, fieldMetadata, true);
                var count = aggregationProvider.AggregateByRange();
                collectionOfFacetValues.IncrementCount((int)count);
                if (needToApplyAggregation)
                {
                    //not supported yet.
                    throw new InvalidOperationException("Facet queries that need to apply aggregation should be handled via scanning reader. This code path is not supposed to be reached for such queries.");
                }

                token.ThrowIfCancellationRequested();
            }
        }


        UpdateRangeResults(results, facetsByRange);
        UpdateFacetResults(results, query, facetsByName);
        CompleteFacetCalculationsStage(results, query);

        CoraxIndexReadOperation.QueryPool.Return(ids);
        return results.Values
            .Select(x => x.Result)
            .ToList();


        FieldMetadata GetFieldMetadata(string name) => QueryBuilderHelper.GetFieldMetadata(_allocator, name, _index, _fieldMappings, null,
            _index.Definition.HasDynamicFields,
            _index.Definition.HasDynamicFields ? new Lazy<List<string>>(() => _indexSearcher.GetFields()) : null, exact: true, hasBoost: true);
    }

    private List<FacetResult> ScanningFacetedQuery(Dictionary<string, FacetedQueryParser.FacetResult> results, FacetQuery facetQuery, QueryTimingsScope queryTimings,
        DocumentsOperationContext context,
        Func<string, SpatialField> getSpatialField, CancellationToken token)
    {
        var query = facetQuery.Query;
        Dictionary<string, Dictionary<string, FacetValues>> facetsByName = new();
        Dictionary<string, Dictionary<string, FacetValues>> facetsByRange = new();

        var parameters = new CoraxQueryBuilder.Parameters(_indexSearcher, _allocator, null, null, query, _index, query.QueryParameters, _queryBuilderFactories,
            _fieldMappings, null, null, -1, token: token);
        var baseQuery = CoraxQueryBuilder.BuildQuery(parameters, out _);

        var coraxPageSize = CoraxBufferSize(_indexSearcher, facetQuery.Query.PageSize, query);
        var ids = CoraxIndexReadOperation.QueryPool.Rent(coraxPageSize);

        Page page = default;
        int read = 0;
        CreateMappingForRanges(results, facetsByRange, facetQuery);

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
                        HandleFacetsPerDocument(ref reader, result, facetsByName, facetQuery.Legacy, facetTiming, token);
                        continue;
                    }

                    // Cache facetByRange because we will fulfill data in batches instead of whole collection
                    if (facetsByRange.TryGetValue(result.Key, out var facetValues) == false)
                    {
                        facetValues = new();
                        facetsByRange.Add(result.Key, facetValues);
                    }

                    HandleRangeFacetsPerDocument(ref reader, result.Key, result.Value, facetQuery.Legacy, facetTiming, facetValues, token);
                }
            }

            token.ThrowIfCancellationRequested();
        }

        UpdateRangeResults(results, facetsByRange);

        UpdateFacetResults(results, query, facetsByName);

        CompleteFacetCalculationsStage(results, query);
        CoraxIndexReadOperation.QueryPool.Return(ids);
        return results.Values
            .Select(x => x.Result)
            .ToList();
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

    private void HandleRangeFacetsPerDocument(ref EntryTermsReader reader,
        string name, FacetedQueryParser.FacetResult result,
        bool legacy,
        QueryTimingsScope queryTimings,
        Dictionary<string, FacetValues> facetValues,
        CancellationToken token)
    {
        var needToApplyAggregation = result.Aggregations.Count > 0;
        var ranges = result.Ranges;

        foreach (var parsedRange in ranges)
        {
            if (parsedRange is not FacetedQueryParser.CoraxParsedRange range)
                continue;

            var fieldRootPage = GetFieldRootPage(range.Field);

            bool isMatching = false;
            reader.Reset();
            while (reader.FindNext(fieldRootPage))
            {
                if (reader.IsNull || reader.IsNonExisting)
                    continue;

                isMatching = result.RangeType switch
                {
                    RangeType.Double => range.IsMatch(reader.CurrentDouble),
                    RangeType.Long => range.IsMatch(reader.CurrentLong),
                    _ => range.IsMatch(reader.Current.Decoded())
                };
                break;
            }

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
        QueryTimingsScope queryTimings,
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
