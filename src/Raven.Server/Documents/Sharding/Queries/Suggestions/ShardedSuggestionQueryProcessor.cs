using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Util;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries.Suggestions;

public class ShardedSuggestionQueryProcessor : AbstractShardedQueryProcessor<ShardedQueryCommand, QueryResult, SuggestionQueryResult>
{
    private readonly long? _existingResultEtag;

    private readonly string _raftUniqueRequestId;
    public ShardedSuggestionQueryProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query, long? existingResultEtag, CancellationToken token) : base(context, requestHandler, query, false, false, token)
    {
        _existingResultEtag = existingResultEtag; 
        _raftUniqueRequestId = _requestHandler.GetRaftRequestIdFromQuery() ?? RaftIdGenerator.NewId();
    }

    public override async Task<SuggestionQueryResult> ExecuteShardedOperations()
    {
        var commands = GetOperationCommands();

        Dictionary<string, SuggestionField> fieldsWithOptions = null;

        foreach (var field in _query.Metadata.SelectFields)
        {
            if (field is SuggestionField {HasOptions: true} suggestionField)
            {
                fieldsWithOptions ??= new Dictionary<string, SuggestionField>();

                fieldsWithOptions.Add(suggestionField.Name, suggestionField);
            }
        }

        var operation = new ShardedSuggestionQueryOperation(fieldsWithOptions, _query.QueryParameters, _context, _requestHandler, commands, _existingResultEtag?.ToString());

        var shardedReadResult = await _requestHandler.ShardExecutor.ExecuteParallelForShardsAsync(commands.Keys.ToArray(), operation, _token);

        if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
        {
            return SuggestionQueryResult.NotModifiedResult;
        }

        var result = shardedReadResult.Result;

        if (_isAutoMapReduceQuery && result.RaftCommandIndex.HasValue)
        {
            // we are waiting here for all nodes, we should wait for all of the orchestrators at least to apply that
            // so further queries would not throw index does not exist in case of a failover
            await _requestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(result.RaftCommandIndex.Value);
        }

        return result;
    }

    protected override ShardedQueryCommand CreateCommand(BlittableJsonReaderObject query)
    {
        return new ShardedQueryCommand(_context.ReadObject(query, "query"), _query, _metadataOnly, _indexEntriesOnly, _query.Metadata.IndexName,
            canReadFromCache: _existingResultEtag != null, raftUniqueRequestId: _raftUniqueRequestId);
    }
}
