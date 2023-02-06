using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries.Suggestions;

public class ShardedSuggestionQueryProcessor : AbstractShardedQueryProcessor<ShardedQueryCommand, QueryResult, SuggestionQueryResult>
{


    public ShardedSuggestionQueryProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query,
        long? existingResultEtag, CancellationToken token) : base(context, requestHandler, query, metadataOnly: false,  indexEntriesOnly: false, existingResultEtag, token)
    {
    }

    public override async Task<SuggestionQueryResult> ExecuteShardedOperations()
    {
        var commands = GetOperationCommands();

        Dictionary<string, SuggestionField> fieldsWithOptions = null;

        foreach (var field in Query.Metadata.SelectFields)
        {
            if (field is SuggestionField {HasOptions: true} suggestionField)
            {
                fieldsWithOptions ??= new Dictionary<string, SuggestionField>();

                fieldsWithOptions.Add(suggestionField.Name, suggestionField);
            }
        }

        var operation = new ShardedSuggestionQueryOperation(fieldsWithOptions, Query.QueryParameters, Context, RequestHandler, commands, ExistingResultEtag?.ToString());

        var shards = GetShardNumbers(commands);

        var shardedReadResult = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shards, operation, Token);

        if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
        {
            return SuggestionQueryResult.NotModifiedResult;
        }

        var result = shardedReadResult.Result;

        await WaitForRaftIndexIfNeededAsync(result.RaftCommandIndex);

        return result;
    }

    protected override ShardedQueryCommand CreateCommand(BlittableJsonReaderObject query) => CreateShardedQueryCommand(query);
}
