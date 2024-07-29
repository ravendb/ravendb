using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations.Queries;

public sealed class ShardedFacetedQueryOperation : AbstractShardedQueryOperation<FacetedQueryResult, FacetResult, Document>
{
    private readonly Dictionary<string, FacetOptions> _facetOptions;

    public ShardedFacetedQueryOperation(IndexQueryServerSide query, Dictionary<string, FacetOptions> facetOptions, TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler,
        Dictionary<int, ShardedQueryCommand> queryCommands, string expectedEtag)
        : base(query.Metadata, queryCommands, context, requestHandler, expectedEtag)
    {
        _facetOptions = facetOptions;
    }

    public override FacetedQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new FacetedQueryResult
        {
            ResultEtag = CombinedResultEtag,
            IsStale = HadActiveMigrationsBeforeQueryStarted
        };

        var facets = new Dictionary<string, CombinedFacet>();

        var deserializer = DocumentConventions.DefaultForServer.Serialization.DefaultConverter;

        foreach (var (shardNumber, cmdResult) in results)
        {
            var queryResult = cmdResult.Result;

            CombineExplanations(result, cmdResult);
            CombineTimings(shardNumber, cmdResult);
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
        result.TotalResults = result.Results.Count;

        return result;
    }

    private sealed class CombinedFacet
    {
        private readonly FacetOptions _options;
        private FacetResult _combined;

        private readonly Dictionary<string, IndexFacetReadOperationBase.FacetValues> _values = new();

        public CombinedFacet(FacetOptions options)
        {
            _options = options;
        }

        public void Add(FacetResult facetResult)
        {
            _combined ??= new FacetResult
            {
                Name = facetResult.Name,
                RemainingHits = facetResult.RemainingHits,
                RemainingTerms = facetResult.RemainingTerms,
                RemainingTermsCount = facetResult.RemainingTermsCount
            };

            foreach (var value in facetResult.Values)
            {
                if (_values.TryGetValue(value.Range, out var values) == false)
                {
                    _values[value.Range] = values = new IndexFacetReadOperationBase.FacetValues(false);
                }

                var field = string.IsNullOrEmpty(value.Name) == false ? new FacetAggregationField { Name = value.Name } : IndexFacetReadOperationBase.FacetValues.Default;

                if (values.TryGet(field, out var facetValue) == false)
                {
                    facetValue = new FacetValue
                    {
                        Average = value.Average,
                        Count = value.Count,
                        Max = value.Max,
                        Min = value.Min,
                        Name = value.Name,
                        Range = value.Range,
                        Sum = value.Sum
                    };

                    values.Add(field, facetValue);
                }
                else
                    UpdateFacetValue(ref facetValue, value);

                values.Count = facetValue.Count;
            }

            static void UpdateFacetValue(ref FacetValue facetValue, FacetValue value)
            {
                facetValue.Count += value.Count;

                if (value.Average is not null)
                    facetValue.Average = facetValue.Average is null ? value.Average : facetValue.Average + value.Average; // we'll convert it to avg value at the end of processing

                if (value.Max > facetValue.Max)
                    facetValue.Max = value.Max;

                if (value.Min < facetValue.Min)
                    facetValue.Min = value.Min;

                if (value.Sum is not null)
                    facetValue.Sum = facetValue.Sum is null ? value.Sum : facetValue.Sum + value.Sum;
            }
        }

        public FacetResult GetResult()
        {
            if (_options != null)
            {
                List<string> allTerms = IndexFacetReadOperationBase.GetAllTermsSorted(_options.TermSortMode, null, _values);

                var start = _options.Start;
                var pageSize = Math.Min(allTerms.Count, _options.PageSize);

                var values = IndexFacetReadOperationBase.GetSortedAndPagedFacetValues(allTerms, start, pageSize, _values, UpdateAverageValue);

                _combined.Values = values.Values;
                _combined.RemainingTermsCount = values.RemainingTermsCount;
                _combined.RemainingHits = values.RemainingHits;

                if (_options.IncludeRemainingTerms)
                    _combined.RemainingTerms = allTerms.Skip(start + values.ValuesCount).ToList();
            }
            else
            {
                var values = new List<FacetValue>();

                foreach (var (_, value) in _values)
                {
                    foreach (var facetValue in value.GetAll())
                    {
                        UpdateAverageValue(facetValue);

                        values.Add(facetValue);
                    }
                }

                _combined.Values = values;
            }

            return _combined;

            static void UpdateAverageValue(FacetValue value)
            {
                if (value.Average.HasValue == false)
                    return;

                if (value.Count == 0)
                    value.Average = double.NaN;
                else
                    value.Average /= value.Count;
            }
        }
    }
}
