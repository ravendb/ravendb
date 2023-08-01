using System;
using System.Collections.Generic;
using System.Linq;
using NuGet.Packaging;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Queries.Suggestions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations.Queries;

public sealed class ShardedSuggestionQueryOperation : AbstractShardedQueryOperation<SuggestionQueryResult, SuggestionResult, Document>
{
    private readonly Dictionary<string, SuggestionField> _fieldsWithOptions;
    private readonly BlittableJsonReaderObject _queryParameters;

    public ShardedSuggestionQueryOperation(IndexQueryServerSide query, Dictionary<string, SuggestionField> fieldsWithOptions, BlittableJsonReaderObject queryParameters,
        TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, Dictionary<int, ShardedQueryCommand> queryCommands, string expectedEtag)
        : base(query.Metadata, queryCommands, context, requestHandler, expectedEtag)
    {
        _fieldsWithOptions = fieldsWithOptions;
        _queryParameters = queryParameters;
    }

    public override SuggestionQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new SuggestionQueryResult
        {
            ResultEtag = CombinedResultEtag,
            IsStale = HadActiveMigrationsBeforeQueryStarted
        };

        var suggestions = new Dictionary<string, CombinedSuggestions>();

        var deserializer = DocumentConventions.DefaultForServer.Serialization.DefaultConverter;

        foreach (var (shardNumber, cmdResult) in results)
        {
            var queryResult = cmdResult.Result;

            CombineExplanations(result, cmdResult);
            CombineTimings(shardNumber, cmdResult);
            CombineSingleShardResultProperties(result, queryResult);

            foreach (BlittableJsonReaderObject suggestionJson in cmdResult.Result.Results)
            {
                var suggestionResult = deserializer.FromBlittable<SuggestionResult>(suggestionJson, "suggestion/result");

                var fieldName = suggestionResult.Name;

                if (suggestionResult.Suggestions.Count == 0)
                {
                    if (suggestions.ContainsKey(fieldName) == false)
                        suggestions[fieldName] = new CombinedSuggestions();

                    continue;
                }

                BlittableJsonReaderObject popularityMetadata = null;

                if (suggestionJson.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                    metadata.TryGet(Constants.Documents.Metadata.Sharding.Querying.SuggestionsPopularityFields, out popularityMetadata);


                if (suggestions.TryGetValue(fieldName, out CombinedSuggestions combinedSuggestions) == false)
                {
                    combinedSuggestions = new CombinedSuggestions();
                    suggestions[fieldName] = combinedSuggestions;
                }

                combinedSuggestions.Suggestions.AddRange(suggestionResult.Suggestions);

                if (popularityMetadata is { Count: > 0 })
                    AddPopularity(popularityMetadata, combinedSuggestions, suggestionResult);
            }
        }

        result.Results = new List<SuggestionResult>(suggestions.Count);

        foreach (var suggestion in suggestions)
        {
            var fieldName = suggestion.Key;

            IEnumerable<string> combinedSuggestions;

            if (suggestion.Value.SuggestionsWithPopularity is null)
                combinedSuggestions = suggestion.Value.Suggestions;
            else
                combinedSuggestions = suggestion.Value.SuggestionsWithPopularity.Values.OrderByDescending(suggestWord => suggestWord).Select(suggestWord => suggestWord.Term).ToArray();

            var fieldResult = new SuggestionResult { Name = fieldName };

            if (_fieldsWithOptions?.TryGetValue(fieldName, out var field) == true)
            {
                var options = field.GetOptions(Context, _queryParameters);

                fieldResult.Suggestions = combinedSuggestions.Take(options.PageSize).ToList();
            }
            else
                fieldResult.Suggestions = combinedSuggestions.ToList();

            result.Results.Add(fieldResult);
        }

        return result;

        void AddPopularity(BlittableJsonReaderObject popularityObject, CombinedSuggestions aggregatedSuggestion, SuggestionResult suggestionResult)
        {
            var i = 0;

            aggregatedSuggestion.SuggestionsWithPopularity ??= new Dictionary<string, SuggestWord>();

            var popularity = deserializer.FromBlittable<ShardedSuggestionResult.Popularity>(popularityObject, "suggestion/popularity");

            foreach (var p in popularity.Values)
            {
                aggregatedSuggestion.AddSuggestionPopularity(suggestionResult.Suggestions[i++], p);
            }
        }
    }

    private sealed class CombinedSuggestions
    {
        public HashSet<string> Suggestions { get; } = new();

        public Dictionary<string, SuggestWord> SuggestionsWithPopularity { get; set; }

        public void AddSuggestionPopularity(string suggestion, SuggestWord popularity)
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
