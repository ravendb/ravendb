using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax;
using Corax.Mappings;
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

        var parameters = new CoraxQueryBuilder.Parameters(_indexSearcher, _allocator, null, null, query, _index, query.QueryParameters, _queryBuilderFactories, _fieldMappings, null, null, -1, null);
        var baseQuery = CoraxQueryBuilder.BuildQuery(parameters);
        var coraxPageSize = CoraxBufferSize(_indexSearcher, facetQuery.Query.PageSize, query);
        var ids = CoraxIndexReadOperation.QueryPool.Rent(coraxPageSize);

        using var analyzersScope = new AnalyzersScope(_indexSearcher, _fieldMappings, _index.Definition.HasDynamicFields);

        int read = 0;
        while ((read = baseQuery.Fill(ids)) != 0)
        {
            for (int docId = 0; docId < read; docId++)
            {
                var entryReader = _indexSearcher.GetEntryReaderFor(ids[docId]);
                foreach (var result in results)
                {
                    token.ThrowIfCancellationRequested();

                    using var facetTiming = queryTimings?.For($"{nameof(QueryTimingsScope.Names.AggregateBy)}/{result.Key}");

                    if (result.Value.Ranges == null || result.Value.Ranges.Count == 0)
                    {
                        HandleFacetsPerDocument(ref entryReader, result, facetsByName, facetQuery.Legacy, facetTiming, analyzersScope, token);
                        continue;
                    }

                    // Cache facetByRange because we will fulfill data in batches instead of whole collection
                    if (facetsByRange.TryGetValue(result.Key, out var facetValues) == false)
                    {
                        facetValues = new();
                        facetsByRange.Add(result.Key, facetValues);
                    }

                    HandleRangeFacetsPerDocument(ref entryReader, result.Key, result.Value, facetQuery.Legacy, facetTiming, facetValues, token);
                }
            }

            token.ThrowIfCancellationRequested();
        }
        
        UpdateRangeResults();
        
        UpdateFacetResults(results, query, facetsByName);

        CompleteFacetCalculationsStage(results);
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

    private void HandleRangeFacetsPerDocument(ref IndexEntryReader indexEntry,
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

            GetFieldReader(ref indexEntry, in range.Field, out var fieldReader);

            //We don't have any correct way to read it from entry, lets skip it
            if (fieldReader.Type == IndexEntryFieldType.Invalid)
                continue;

            var rangeType = result.RangeType;
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
                        if ((fieldReader.Type & IndexEntryFieldType.HasNulls) != 0 && (iterator.IsEmptyCollection || iterator.IsNull))
                        {
                            var value = iterator.IsEmptyCollection ? Constants.EmptyStringSlice : Constants.NullValueSlice;
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
                        if ((fieldReader.Type & IndexEntryFieldType.HasNulls) != 0 && (tupleIterator.IsEmptyCollection || tupleIterator.IsNull))
                        {
                            var value = tupleIterator.IsEmptyCollection ? Constants.EmptyStringSlice : Constants.NullValueSlice;
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
                    ApplyAggregation(result.Aggregations, collectionOfFacetValues, ref indexEntry);
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

    private void HandleFacetsPerDocument(ref IndexEntryReader entryReader,
        KeyValuePair<string, FacetedQueryParser.FacetResult> result,
        Dictionary<string, Dictionary<string, FacetValues>> facetsByName,
        bool legacy,
        QueryTimingsScope queryTimings,
        AnalyzersScope analyzersScope,
        CancellationToken token)
    {
        var needToApplyAggregation = result.Value.Aggregations.Count > 0;
        if (facetsByName.TryGetValue(result.Key, out var facetValues) == false)
            facetsByName[result.Key] = facetValues = new Dictionary<string, FacetValues>();

        GetFieldReader(ref entryReader, result.Value.AggregateBy, out var fieldReader);

        switch (fieldReader.Type)
        {
            case IndexEntryFieldType.ListWithEmpty:
            case IndexEntryFieldType.ListWithNulls:
            case IndexEntryFieldType.List:
            case IndexEntryFieldType.TupleList:
            case IndexEntryFieldType.TupleListWithNulls:
                if (fieldReader.TryReadMany(out var iterator)==false)
                    break;

                while (iterator.ReadNext())
                {
                    if (iterator.IsNull || iterator.IsEmptyCollection)
                    {
                        facetValues[iterator.IsNull ? Constants.NullValue : Constants.EmptyString].IncrementCount(1);
                        continue;
                    }
                    InsertTerm(iterator.Sequence, ref entryReader);
                }
        
                break;
            case IndexEntryFieldType.Tuple:
            case IndexEntryFieldType.Simple:
                if (fieldReader.Read(out var source))
                    InsertTerm(source, ref entryReader);
                break;
            case IndexEntryFieldType.Null:
                break;
            default:
                throw new Exception($"Got type {fieldReader.Type}");
        }
        
         void InsertTerm(ReadOnlySpan<byte> source, ref IndexEntryReader entryReader)
         {
             var nameAsSlice = GetFieldNameAsSlice(result.Value.AggregateBy);
             analyzersScope.Execute(nameAsSlice, source, out var buffer, out var tokens);

             foreach (var tokenOutput in tokens)
             {
                 token.ThrowIfCancellationRequested();
                 var term = buffer.Slice(tokenOutput.Offset, (int)tokenOutput.Length);
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
                     ApplyAggregation(result.Value.Aggregations, collectionOfFacetValues, ref entryReader);
                 }
             }
         }
    }

    private void GetFieldReader(ref IndexEntryReader reader, in string name, out IndexEntryReader.FieldReader fieldReader)
    {
        if (_fieldMappings.TryGetByFieldName(_allocator, name, out var binding))
        {
            // In this case we've to check if field is dynamic also
            fieldReader = reader.GetFieldReaderFor(binding.FieldId);
            return;
        }

        var slicedFieldName = GetFieldNameAsSlice(name);
        fieldReader = reader.GetFieldReaderFor(slicedFieldName);
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
            GetFieldReader(ref entryReader, name, out var fieldReader);
            switch (fieldReader.Type)
            {
                case IndexEntryFieldType.TupleList:
                case IndexEntryFieldType.TupleListWithNulls:
                    if (fieldReader.TryReadMany(out var tupleIterator) == false)
                        goto default;

                    while (tupleIterator.ReadNext())
                    {
                        if ((fieldReader.Type & IndexEntryFieldType.HasNulls) != 0 && (tupleIterator.IsEmptyCollection || tupleIterator.IsNull))
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
