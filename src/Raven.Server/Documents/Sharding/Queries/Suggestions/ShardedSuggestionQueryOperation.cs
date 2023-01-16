using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using NuGet.Packaging;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries.Suggestions;

public class ShardedSuggestionQueryOperation : IShardedReadOperation<QueryResult, SuggestionQueryResult>
{
    private readonly ShardedDatabaseRequestHandler _requestHandler;
    private readonly Dictionary<int, ShardedQueryCommand> _queryCommands;
    private long _combinedResultEtag;
    private readonly IndexQueryServerSide _query;
    private readonly TransactionOperationContext _context;

    public ShardedSuggestionQueryOperation(IndexQueryServerSide query, TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, Dictionary<int, ShardedQueryCommand> queryCommands, string expectedEtag)
    {
        _query = query;
        _context = context;
        _requestHandler = requestHandler;
        _queryCommands = queryCommands;
        ExpectedEtag = expectedEtag;
    }

    public string ExpectedEtag { get; }

    public HttpRequest HttpRequest { get => _requestHandler.HttpContext.Request; }

    RavenCommand<QueryResult> IShardedOperation<QueryResult, ShardedReadResult<SuggestionQueryResult>>.CreateCommandForShard(int shardNumber) => _queryCommands[shardNumber];

    public string CombineCommandsEtag(Dictionary<int, ShardExecutionResult<QueryResult>> commands)
    {
        _combinedResultEtag = 0;

        foreach (var cmd in commands.Values)
        {
            _combinedResultEtag = Hashing.Combine(_combinedResultEtag, cmd.Result.ResultEtag);
        }

        return CharExtensions.ToInvariantString(_combinedResultEtag);
    }

    public SuggestionQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new SuggestionQueryResult
        {
            ResultEtag = _combinedResultEtag
        };

        var suggestions = new Dictionary<string, AggregatedSuggestions>();

        var deserializer = DocumentConventions.DefaultForServer.Serialization.DefaultConverter;

        foreach (var cmdResult in results.Values)
        {
            var queryResult = cmdResult.Result;

            ShardedQueryOperation.CombineSingleShardResultProperties(result, queryResult);

            foreach (BlittableJsonReaderObject suggestionJson in cmdResult.Result.Results)
            {
                var suggestionResult = deserializer.FromBlittable<SuggestionResult>(suggestionJson, "suggestion/result");

                var fieldName = suggestionResult.Name;

                if (suggestionResult.Suggestions.Count == 0)
                {
                    if (suggestions.ContainsKey(fieldName) == false)
                        suggestions[fieldName] = new AggregatedSuggestions();

                    continue;
                }

                BlittableJsonReaderArray popularityMetadata = null;

                if (suggestionJson.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                    metadata.TryGet(Constants.Documents.Metadata.SuggestionPopularityFields, out popularityMetadata);


                if (suggestions.TryGetValue(fieldName, out AggregatedSuggestions aggregatedSuggestion) == false)
                {
                    aggregatedSuggestion = new AggregatedSuggestions();
                    suggestions[fieldName] = aggregatedSuggestion;
                }
                else
                {
                    aggregatedSuggestion = suggestions[fieldName];
                }

                aggregatedSuggestion.Suggestions.AddRange(suggestionResult.Suggestions);

                if (popularityMetadata is { Length: > 0 })
                    AddPopularity(popularityMetadata, aggregatedSuggestion, suggestionResult);
            }
        }

        var suggestionOptions = _query.Metadata.SelectFields.Where(x => x.IsSuggest)
                                                                        .Cast<SuggestionField>()
                                                                        .Where(x => x.HasOptions)
                                                                        .ToDictionary(x => x.Name.Value, x => x);

        result.Results = new List<SuggestionResult>(suggestions.Count);

        foreach (var suggestion in suggestions)
        {
            var fieldName = suggestion.Key;

            IEnumerable<string> aggregatedSuggestions;

            if (suggestion.Value.SuggestionsWithPopularity is null)
                aggregatedSuggestions = suggestion.Value.Suggestions;
            else
                aggregatedSuggestions = suggestion.Value.SuggestionsWithPopularity.Values.OrderByDescending(suggestWord => suggestWord).Select(suggestWord => suggestWord.Term).ToArray();

            var fieldResult = new SuggestionResult { Name = fieldName };

            if (suggestionOptions.TryGetValue(fieldName, out var field) == false)
                fieldResult.Suggestions = aggregatedSuggestions.ToList();
            else
            {
                var options = field.GetOptions(_context, _query.QueryParameters);

                fieldResult.Suggestions = aggregatedSuggestions.Take(options.PageSize).ToList();
            }

            result.Results.Add(fieldResult);
        }

        return result;

        void AddPopularity(BlittableJsonReaderArray popularityArray, AggregatedSuggestions aggregatedSuggestion, SuggestionResult suggestionResult)
        {
            var i = 0;

            aggregatedSuggestion.SuggestionsWithPopularity ??= new Dictionary<string, SuggestWord>();

            foreach (BlittableJsonReaderObject pJson in popularityArray.Cast<BlittableJsonReaderObject>())
            {
                var p = deserializer.FromBlittable<SuggestionResultWithPopularity.Popularity>(pJson, "suggestion/popularity");

                aggregatedSuggestion.AddSuggestionPopularity(suggestionResult.Suggestions[i++], p);
            }
        }
    }

    private class AggregatedSuggestions
    {
        public HashSet<string> Suggestions { get; } = new();

        public Dictionary<string, SuggestWord> SuggestionsWithPopularity { get; set; }

        public void AddSuggestionPopularity(string suggestion, SuggestionResultWithPopularity.Popularity popularity)
        {
            var suggestWord = new SuggestWord
            {
                Term = suggestion,
                Freq = popularity.Freq,
                Score = popularity.Score
            };

            if (SuggestionsWithPopularity.TryAdd(suggestion, suggestWord) == false)
            {
                SuggestionsWithPopularity[suggestion].Freq += popularity.Freq;
                SuggestionsWithPopularity[suggestion].Score = Math.Max(SuggestionsWithPopularity[suggestion].Score, popularity.Score); // score is calculated based on distance so we get the highest value
            }
        }
    }
}
