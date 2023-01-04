using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
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

        var suggestions = new Dictionary<string, HashSet<string>>();

        foreach (var cmdResult in results.Values)
        {
            // TODO arek
            var queryRes = cmdResult.Result;
            result.TotalResults += queryRes.TotalResults;
            result.IsStale |= queryRes.IsStale;
            result.SkippedResults += queryRes.SkippedResults;
            result.IndexName = queryRes.IndexName;
            result.IncludedPaths = queryRes.IncludedPaths;

            if (result.IndexTimestamp < queryRes.IndexTimestamp)
                result.IndexTimestamp = queryRes.IndexTimestamp;

            if (result.LastQueryTime < queryRes.LastQueryTime)
                result.LastQueryTime = queryRes.LastQueryTime;

            if (queryRes.RaftCommandIndex.HasValue)
            {
                if (result.RaftCommandIndex == null || queryRes.RaftCommandIndex > result.RaftCommandIndex)
                    result.RaftCommandIndex = queryRes.RaftCommandIndex;
            }

            foreach (BlittableJsonReaderObject suggestion in cmdResult.Result.Results)
            {
                var suggestionResult = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<SuggestionResult>(suggestion, "suggestion/result"); // TODO arek

                if (suggestionResult.Suggestions.Count == 0 )
                    continue;

                if (suggestions.ContainsKey(suggestionResult.Name) == false)
                    suggestions[suggestionResult.Name] = new HashSet<string>(suggestionResult.Suggestions);
                else
                {
                    foreach (string s in suggestionResult.Suggestions)
                    {
                        suggestions[suggestionResult.Name].Add(s);
                    }
                }
            }
        }

        result.Results = suggestions.Select(x => new SuggestionResult
        {
            Name = x.Key,
            Suggestions = x.Value.ToList()
        }).ToList();

        return result;
    }
}
