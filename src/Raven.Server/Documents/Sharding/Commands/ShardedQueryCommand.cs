using System.Net.Http;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands;

public class ShardedQueryCommand : ShardedBaseCommand<QueryResult>
{
    private readonly string _indexName;

    public ShardedQueryCommand(ShardedDatabaseRequestHandler handler, HttpMethod method, BlittableJsonReaderObject content, string indexName) : base(handler, method, Commands.Headers.Sharded, content)
    {
        _indexName = indexName;
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
        {
            // is null only when index doesn't exist
            throw new IndexDoesNotExistException($"Index `{_indexName}` was not found");
        }

        Result = JsonDeserializationClient.QueryResult(response);
    }
}
