using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax;
using Corax.Mappings;
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
using IndexEntryReader = Corax.IndexEntryReader;
using RangeType = Raven.Client.Documents.Indexes.RangeType;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxIndexFacetedReadOperation : IndexFacetReadOperationBase
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
        _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings);
        _fieldNameCache = new();
    }

    public override List<FacetResult> FacetedQuery(FacetQuery facetQuery, QueryTimingsScope queryTimings, DocumentsOperationContext context,
        Func<string, SpatialField> getSpatialField, CancellationToken token)
    {
        var results = FacetedQueryParser.Parse(context, facetQuery, SearchEngineType.Corax);

        var query = facetQuery.Query;
        Dictionary<string, Dictionary<string, FacetValues>> facetsByName = new();
        Dictionary<string, Dictionary<string, FacetValues>> facetsByRange = new();

        var parameters = new CoraxQueryBuilder.Parameters(_indexSearcher, _allocator, null, null, query, _index, query.QueryParameters, _queryBuilderFactories, _fieldMappings, null, null, -1, token: token);
        var baseQuery = CoraxQueryBuilder.BuildQuery(parameters, out _);
        var coraxPageSize = CoraxBufferSize(_indexSearcher, facetQuery.Query.PageSize, query);
        var ids = CoraxIndexReadOperation.QueryPool.Rent(coraxPageSize);

        Page page = default;
        int read = 0;
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

        UpdateRangeResults();

        UpdateFacetResults(results, query, facetsByName);

        CompleteFacetCalculationsStage(results, query);
        CoraxIndexReadOperation.QueryPool.Return(ids);
        return results.Values
            .Select(x => x.Result)
            .ToList();

        void UpdateRangeResults()
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

        // Create map in first batch
        if (facetValues.Count == 0)
        {
            CreateFacetMapping();
        }

        foreach (var parsedRange in ranges)
        {
            if (parsedRange is not FacetedQueryParser.CoraxParsedRange range)
                continue;

            var fieldRootPage = GetFieldRootPage(range.Field);

            bool isMatching = false;
            reader.Reset();
            while (reader.FindNext(fieldRootPage))
            {
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


        void CreateFacetMapping()
        {
            foreach (var range in ranges)
            {
                var key = range.RangeText;
                if (facetValues.TryGetValue(key, out var collectionOfFacetValues))
                    continue;

                collectionOfFacetValues = new FacetValues(legacy);
                if (needToApplyAggregation == false)
                    collectionOfFacetValues.AddDefault(key);
                else
                {
                    foreach (var aggregation in result.Aggregations)
                        collectionOfFacetValues.Add(aggregation.Key, key);
                }

                facetValues.Add(key, collectionOfFacetValues);
            }
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

        long fieldRootPage = GetFieldRootPage(result.Value.AggregateBy);

        var cloned = reader;
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            InsertTerm(reader.Current.Decoded(), ref cloned);
        } 

        void InsertTerm(ReadOnlySpan<byte> term, ref EntryTermsReader reader)
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
    }

    private long GetFieldRootPage(string fieldName)
    {
        ref var fieldRootPage = ref CollectionsMarshal.GetValueRefOrAddDefault(_fieldNameToRootPage, fieldName, out var exists);
        if (exists == false)
        {
            fieldRootPage = _indexSearcher.GetLookupRootPage(fieldName);
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Slice GetFieldNameAsSlice(string fieldName)
    {
        // Slices will exists as long the transaction so we don't have to worry about memoryleak here.
        if (_fieldNameCache.TryGetValue(fieldName, out var value) == false)
        {
            Slice.From(_allocator, fieldName, ByteStringType.Immutable, out value);
            _fieldNameCache.Add(fieldName, value);
        }

        return value;
    }
}
