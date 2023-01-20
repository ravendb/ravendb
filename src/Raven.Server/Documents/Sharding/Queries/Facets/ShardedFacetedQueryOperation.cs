using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using static Corax.Constants;

namespace Raven.Server.Documents.Sharding.Queries.Facets;

public class ShardedFacetedQueryOperation : IShardedReadOperation<QueryResult, FacetedQueryResult>
{
    private readonly ShardedDatabaseRequestHandler _requestHandler;
    private readonly Dictionary<int, ShardedQueryCommand> _queryCommands;
    private long _combinedResultEtag;
    private readonly Dictionary<string, FacetOptions> _facetOptions;
    private readonly IndexQueryServerSide _query;
    private readonly TransactionOperationContext _context;

    public ShardedFacetedQueryOperation(Dictionary<string, FacetOptions> facetOptions, IndexQueryServerSide query, TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, Dictionary<int, ShardedQueryCommand> queryCommands, string expectedEtag)
    {
        _facetOptions = facetOptions;
        _query = query;
        _context = context;
        _requestHandler = requestHandler;
        _queryCommands = queryCommands;
        ExpectedEtag = expectedEtag;
    }

    public string ExpectedEtag { get; }

    public HttpRequest HttpRequest { get => _requestHandler.HttpContext.Request; }

    RavenCommand<QueryResult> IShardedOperation<QueryResult, ShardedReadResult<FacetedQueryResult>>.CreateCommandForShard(int shardNumber) => _queryCommands[shardNumber];

    public string CombineCommandsEtag(Dictionary<int, ShardExecutionResult<QueryResult>> commands)
    {
        _combinedResultEtag = 0;

        foreach (var cmd in commands.Values)
        {
            _combinedResultEtag = Hashing.Combine(_combinedResultEtag, cmd.Result.ResultEtag);
        }

        return CharExtensions.ToInvariantString(_combinedResultEtag);
    }

    public FacetedQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new FacetedQueryResult
        {
            ResultEtag = _combinedResultEtag
        };

        var facets = new Dictionary<string, CombinedFacet>();

        var deserializer = DocumentConventions.DefaultForServer.Serialization.DefaultConverter;

        foreach (var cmdResult in results.Values)
        {
            var queryResult = cmdResult.Result;

            ShardedQueryOperation.CombineSingleShardResultProperties(result, queryResult);

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

        private Dictionary<string, FacetValue> _valuesByRanges;

        public CombinedFacet(FacetOptions options)
        {
            _options = options;
        }

        public void Add(FacetResult facetResult)
        {
            if (_combined == null)
            {
                _combined = new FacetResult
                {
                    Name = facetResult.Name,
                    RemainingHits = facetResult.RemainingHits,
                    RemainingTerms = facetResult.RemainingTerms,
                    RemainingTermsCount = facetResult.RemainingTermsCount,
                    Values = new List<FacetValue>(facetResult.Values.Count)
                };

                _valuesByRanges = new Dictionary<string, FacetValue>(facetResult.Values.Count);

                foreach (FacetValue value in facetResult.Values)
                {
                    _valuesByRanges.Add(value.Range, new FacetValue
                    {
                        Average = value.Average,
                        Count = value.Count,
                        Max = value.Max,
                        Min = value.Min,
                        Name = value.Name,
                        Range = value.Range,
                        Sum = value.Sum
                    });
                }
            }
            else
            {
                foreach (FacetValue value in facetResult.Values)
                {
                    if (_valuesByRanges.TryGetValue(value.Range, out var existing) == false)
                    {
                        existing = new FacetValue
                        {
                            Average = value.Average,
                            Count = value.Count,
                            Max = value.Max,
                            Min = value.Min,
                            Name = value.Name,
                            Range = value.Range,
                            Sum = value.Sum
                        };

                        _valuesByRanges[value.Range] = existing;
                    }
                    else
                    {
                        existing.Count += value.Count;
                        existing.Average += value.Average; // we'll convert it to avg value at the end of processing

                        if (value.Max > existing.Max)
                            existing.Max = value.Max;

                        if (value.Min < existing.Min)
                            existing.Min = value.Min;

                        existing.Sum += value.Sum;
                    }
                }
            }
        }

        public FacetResult GetResult()
        {
            if (_options != null)
            {
                var valuesCount = 0;
                var valuesSumOfCounts = 0;
                var values = new List<FacetValue>();
                List<string> allTerms = IndexFacetReadOperationBase.GetAllTermsSorted(_options.TermSortMode, _valuesByRanges, x => x.Value.Count);

                var start = _options.Start;
                var pageSize = Math.Min(allTerms.Count, _options.PageSize);

                foreach (var term in allTerms.Skip(start).TakeWhile(term => valuesCount < pageSize))
                {
                    valuesCount++;
                    
                    var facetValues = _valuesByRanges[term];

                    values.Add(facetValues);

                    valuesSumOfCounts += facetValues.Count;
                }

                var previousHits = allTerms.Take(start).Sum(allTerm =>
                {
                    if (_valuesByRanges.TryGetValue(allTerm, out var facetValues) == false || facetValues == null || facetValues.Count == 0)
                        return 0;

                    return facetValues.Count;
                });

                _combined.Values = values;
                _combined.RemainingTermsCount = allTerms.Count - (start + valuesCount);
                _combined.RemainingHits = _valuesByRanges.Values.Sum(x => x.Count) - (previousHits + valuesSumOfCounts);

                if (_options.IncludeRemainingTerms)
                    _combined.RemainingTerms = allTerms.Skip(start + valuesCount).ToList();
            }
            else
            {
                foreach (var item in _valuesByRanges)
                {
                    var value = item.Value;

                    if (value.Average.HasValue)
                    {
                        if (value.Count == 0)
                            value.Average = double.NaN;
                        else
                            value.Average /= value.Count;
                    }

                    _combined.Values.Add(value);
                }
            }
            return _combined;
        }
    }
}
