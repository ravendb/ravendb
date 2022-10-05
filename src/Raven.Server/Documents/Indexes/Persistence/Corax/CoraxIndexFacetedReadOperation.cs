using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax;
using Nest;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Impl;
using RangeType = Raven.Client.Documents.Indexes.RangeType;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxIndexFacetedReadOperation : IndexFacetReadOperationBase
{
    private readonly IndexFieldsMapping _fieldMappings;
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
        Dictionary<string, Dictionary<string, FacetValues>> facetsByName = null;
        Dictionary<string, Dictionary<string, FacetValues>> facetsByRange = null;


        //Build corax query to prefilter before aggregations happens
        var parameters = new CoraxQueryBuilder.Parameters(_indexSearcher, null, null, query, _index, query.QueryParameters, _queryBuilderFactories,
            _fieldMappings, null, null, -1, null);
        var baseQuery = CoraxQueryBuilder.BuildQuery(parameters, out var isBinary);
        var coraxPageSize = CoraxGetPageSize(_indexSearcher, facetQuery.Query.PageSize, query, isBinary);
        var ids = CoraxIndexReadOperation.QueryPool.Rent(coraxPageSize);

        int read = 0;
        while ((read = baseQuery.Fill(ids)) != 0)
        {
            foreach (var result in results)
            {
                using (var facetTiming = queryTimings?.For($"{nameof(QueryTimingsScope.Names.AggregateBy)}/{result.Key}"))
                {
                    if (result.Value.Ranges == null || result.Value.Ranges.Count == 0)
                    {
                        facetsByName ??= new Dictionary<string, Dictionary<string, FacetValues>>();

                        // HandleFacets(returnedReaders, result, facetsByName, facetQuery.Legacy, facetTiming, token);
                        continue;
                    }


                    // Cache facetByRange because we will fulfill data in batches instead of whole collection
                    facetsByRange ??= new();
                    if (facetsByRange.TryGetValue(result.Key, out var facetValues) == false)
                    {
                        facetValues = new();
                        facetsByRange.Add(result.Key, facetValues);
                    }

                    HandleRangeFacets(ids.AsSpan()[..read], result, facetQuery.Legacy, facetTiming, facetValues, token);
                }
            }
        }

        CompleteFacetCalculationsStage(results);
        CoraxIndexReadOperation.QueryPool.Return(ids);
        return results.Values
            .Select(x => x.Result)
            .ToList();
    }

    private void HandleRangeFacets(ReadOnlySpan<long> ids,
        KeyValuePair<string, FacetedQueryParser.FacetResult> result,
        bool legacy,
        QueryTimingsScope queryTimings,
        Dictionary<string, FacetValues> facetValues,
        CancellationToken token)
    {
        var needToApplyAggregation = result.Value.Aggregations.Count > 0;
        var ranges = result.Value.Ranges;


        // Create map in first batch
        if (facetValues.Count == 0)
        {
            CreateFacetMapping();
        }

        foreach (var document in ids)
        {
            var indexEntry = _indexSearcher.GetReaderFor(document);
            foreach (var parsedRange in CollectionsMarshal.AsSpan(ranges))
            {
                var range = parsedRange as FacetedQueryParser.CoraxParsedRange;
                if (range is null)
                    continue;
                
                GetFieldReader(ref indexEntry, in range.Field, out var fieldReader);

                //We don't have any correct way to read it from entry, lets skip it
                if (fieldReader.Type == IndexEntryFieldType.Invalid)
                    continue;

                var rangeType = result.Value.RangeType;
                bool isMatching = false;
                switch (fieldReader.Type)
                {
                    case IndexEntryFieldType.List:
                    case IndexEntryFieldType.ListWithNulls:
                    {
                        if (fieldReader.TryReadMany(out var iterator) == false)
                            goto default;
                        
                        while (iterator.ReadNext())
                        {
                            if ((fieldReader.Type & IndexEntryFieldType.HasNulls) != 0 && (iterator.IsEmpty || iterator.IsNull))
                            {
                                var value = iterator.IsEmpty ? Constants.EmptyStringSlice : Constants.NullValueSlice;
                                isMatching |= range.IsMatch(value);
                                continue;
                            }

                            isMatching |= range.IsMatch(iterator.Sequence);
                        }

                        break;
                    }
                    case IndexEntryFieldType.TupleList:
                    case IndexEntryFieldType.TupleListWithNulls:
                        if (fieldReader.TryReadMany(out var tupleIterator) == false)
                            goto default;
                        
                        while (tupleIterator.ReadNext())
                        {
                            if ((fieldReader.Type & IndexEntryFieldType.HasNulls) != 0 && (tupleIterator.IsEmpty || tupleIterator.IsNull))
                            {
                                var value = tupleIterator.IsEmpty ? Constants.EmptyStringSlice : Constants.NullValueSlice;
                                isMatching |= range.IsMatch(value);
                                continue;
                            }

                            isMatching = rangeType switch
                            {
                                RangeType.Double => range.IsMatch(tupleIterator.Double),
                                RangeType.Long => range.IsMatch(tupleIterator.Long),
                                _ => range.IsMatch(tupleIterator.Sequence)
                            };
                        }

                        break;
                    case IndexEntryFieldType.Tuple:
                        fieldReader.Read(out _, out var longValue, out var doubleValue, out var spanValue);
                        isMatching = rangeType switch
                        {
                            RangeType.Double => range.IsMatch(doubleValue),
                            RangeType.Long => range.IsMatch(longValue),
                            _ => range.IsMatch(spanValue)
                        };
                        break;
                    case IndexEntryFieldType.Simple:
                        fieldReader.Read(out _, out spanValue);
                        isMatching = range.IsMatch(spanValue);
                        break;
                    default:
                        break;
                }

                var collectionOfFacetValues = facetValues[range.RangeText];
                if (isMatching)
                {
                    collectionOfFacetValues.IncrementCount(1);
                    if (needToApplyAggregation)
                        ApplyAggregation(result.Value.Aggregations, collectionOfFacetValues, ref indexEntry);
                }
                
                token.ThrowIfCancellationRequested();
            }
        }


        foreach (var kvp in facetValues)
        {
            if (kvp.Value.Any == false)
                continue;

            result.Value.Result.Values.AddRange(kvp.Value.GetAll());
        }

        if (needToApplyAggregation)
        {
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
                    foreach (var aggregation in result.Value.Aggregations)
                        collectionOfFacetValues.Add(aggregation.Key, key);
                }

                facetValues.Add(key, collectionOfFacetValues);
            }
        }
    }

    private void GetFieldReader(ref IndexEntryReader reader, in string name, out IndexEntryReader.FieldReader fieldReader)
    {
        if (_fieldMappings.TryGetByFieldName(name, out var binding))
        {
            // In this case we've to check if field is dynamic also
            fieldReader = reader.GetReaderFor(binding.FieldId);
            return;
        }

        var slicedFieldName = GetFieldNameAsSlice(name);
        fieldReader = reader.GetReaderFor(slicedFieldName);
    }

    private void ApplyAggregation(Dictionary<FacetAggregationField, FacetedQueryParser.FacetResult.Aggregation> aggregations, FacetValues values,
        ref IndexEntryReader entryReader)
    {
        foreach (var kvp in aggregations)
        {
            if (string.IsNullOrEmpty(kvp.Key.Name)) // Count
                continue;

            var value = values.Get(kvp.Key);

            var name = kvp.Key.Name;
            var val = kvp.Value;
            double min = value.Min ?? double.MaxValue, max = value.Max ?? double.MinValue, sum = value.Sum ?? 0, avg = value.Average ?? 0;
            GetFieldReader(ref entryReader, in name, out var fieldReader);
            switch (fieldReader.Type)
            {
                case IndexEntryFieldType.TupleList:
                case IndexEntryFieldType.TupleListWithNulls:
                    if (fieldReader.TryReadMany(out var tupleIterator) == false)
                        goto default;

                    while (tupleIterator.ReadNext())
                    {
                        if ((fieldReader.Type & IndexEntryFieldType.HasNulls) != 0 && (tupleIterator.IsEmpty || tupleIterator.IsNull))
                        {
                            continue;
                        }

                        sum += tupleIterator.Double;
                        avg += tupleIterator.Double;
                        min = Math.Min(min, tupleIterator.Double);
                        max = Math.Max(max, tupleIterator.Double);
                    }

                    break;
                case IndexEntryFieldType.Tuple:
                    fieldReader.Read(out _, out var longValue, out var doubleValue, out var spanValue);
                    sum += doubleValue;
                    avg += doubleValue;
                    min = Math.Min(min, doubleValue);
                    max = Math.Max(max, doubleValue);
                    break;
                default: break;
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
