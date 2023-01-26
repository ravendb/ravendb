using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations.Queries;

public class ShardedFacetedQueryOperation : AbstractShardedQueryOperation<FacetedQueryResult, FacetResult, Document>
{
    private readonly Dictionary<string, FacetOptions> _facetOptions;

    public ShardedFacetedQueryOperation(Dictionary<string, FacetOptions> facetOptions, TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler,
        Dictionary<int, ShardedQueryCommand> queryCommands, string expectedEtag)
        : base(queryCommands, context, requestHandler, expectedEtag)
    {
        _facetOptions = facetOptions;
    }

    public override FacetedQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new FacetedQueryResult
        {
            ResultEtag = CombinedResultEtag
        };

        var facets = new Dictionary<string, CombinedFacet>();

        var deserializer = DocumentConventions.DefaultForServer.Serialization.DefaultConverter;

        foreach (var cmdResult in results.Values)
        {
            var queryResult = cmdResult.Result;

            CombineSingleShardResultProperties(result, queryResult);

            if (queryResult.Includes is { Count: > 0 })
            {
                result.Includes ??= new List<Document>();

                HandleDocumentIncludes(queryResult, result);
            }

            foreach (BlittableJsonReaderObject facetJson in cmdResult.Result.Results)
            {
                var facetResult = deserializer.FromBlittable<FacetResult>(facetJson, "facet/result");

                var fieldName = facetResult.Name;

                if (facets.TryGetValue(fieldName, out CombinedFacet combinedFacet) == false)
                {
                    FacetOptions options = null;
                    _facetOptions?.TryGetValue(fieldName, out options);

                    combinedFacet = new CombinedFacet(options);
                    facets[fieldName] = combinedFacet;
                }

                combinedFacet.Add(facetResult);
            }
        }

        result.Results = facets.Values.Select(x => x.GetResult()).ToList();

        return result;
    }

    private class CombinedFacet
    {
        private readonly FacetOptions _options;
        private FacetResult _combined;

        private Dictionary<string, FacetValue> _rangeValues;
        private Dictionary<string, IndexFacetReadOperationBase.FacetValues> _values;

        public CombinedFacet(FacetOptions options)
        {
            _options = options;
        }

        private bool IsRangeFacet => _options == null; // options aren't applicable for range facets so are always null

        public void Add(FacetResult facetResult)
        {
            _combined ??= new FacetResult
            {
                Name = facetResult.Name,
                RemainingHits = facetResult.RemainingHits,
                RemainingTerms = facetResult.RemainingTerms,
                RemainingTermsCount = facetResult.RemainingTermsCount
            };

            if (IsRangeFacet)
            {
                _rangeValues ??= new Dictionary<string, FacetValue>(facetResult.Values.Count);

                AddRangeFacetResult(facetResult);
            }
            else
            {
                _values ??= new Dictionary<string, IndexFacetReadOperationBase.FacetValues>(facetResult.Values.Count);

                AddFacetResult(facetResult);
            }

            void AddFacetResult(FacetResult result)
            {
                foreach (FacetValue value in result.Values)
                {
                    if (_values.TryGetValue(value.Range, out var values) == false)
                    {
                        _values[value.Range] = values = new IndexFacetReadOperationBase.FacetValues(false);
                    }

                    var field = string.IsNullOrEmpty(value.Name) == false ? new FacetAggregationField { Name = value.Name } : IndexFacetReadOperationBase.FacetValues.Default;

                    if (values.TryGet(field, out var facetValue) == false)
                    {
                        facetValue = CreateFacetValue(value);

                        values.Add(field, facetValue);
                    }
                    else
                    {
                        UpdateFacetValue(ref facetValue, value);
                    }

                    values.Count = facetValue.Count;
                }
            }

            void AddRangeFacetResult(FacetResult result)
            {
                foreach (FacetValue value in result.Values)
                {
                    if (_rangeValues.TryGetValue(value.Range, out var rangeFacetValue) == false)
                    {
                        rangeFacetValue = CreateFacetValue(value);

                        _rangeValues[value.Range] = rangeFacetValue;
                    }
                    else
                    {
                        UpdateFacetValue(ref rangeFacetValue, value);
                    }
                }
            }

            FacetValue CreateFacetValue(FacetValue value)
            {
                return new FacetValue
                {
                    Average = value.Average,
                    Count = value.Count,
                    Max = value.Max,
                    Min = value.Min,
                    Name = value.Name,
                    Range = value.Range,
                    Sum = value.Sum
                };
            }

            void UpdateFacetValue(ref FacetValue facetValue, FacetValue value)
            {
                facetValue.Count += value.Count;
                facetValue.Average += value.Average; // we'll convert it to avg value at the end of processing

                if (value.Max > facetValue.Max)
                    facetValue.Max = value.Max;

                if (value.Min < facetValue.Min)
                    facetValue.Min = value.Min;

                facetValue.Sum += value.Sum;
            }
        }

        public FacetResult GetResult()
        {
            if (IsRangeFacet)
            {
                foreach (var item in _rangeValues)
                {
                    var value = item.Value;

                    UpdateAverageValue(value);

                    _combined.Values.Add(value);
                }
            }
            else
            {
                List<string> allTerms = IndexFacetReadOperationBase.GetAllTermsSorted(_options.TermSortMode, _values);

                var start = _options.Start;
                var pageSize = Math.Min(allTerms.Count, _options.PageSize);

                var values = IndexFacetReadOperationBase.GetSortedAndPagedFacetValues(allTerms, start, pageSize, _values, UpdateAverageValue);

                _combined.Values = values.Values;
                _combined.RemainingTermsCount = values.RemainingTermsCount;
                _combined.RemainingHits = values.RemainingHits;

                if (_options.IncludeRemainingTerms)
                    _combined.RemainingTerms = allTerms.Skip(start + values.ValuesCount).ToList();
            }

            return _combined;

            void UpdateAverageValue(FacetValue value)
            {
                if (value.Average.HasValue)
                {
                    if (value.Count == 0)
                        value.Average = double.NaN;
                    else
                        value.Average /= value.Count;
                }
            }
        }
    }
}
