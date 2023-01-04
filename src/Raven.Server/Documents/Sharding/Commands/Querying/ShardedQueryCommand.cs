using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands.Querying;

public class ShardedQueryCommand : AbstractShardedQueryCommand<QueryResult, BlittableJsonReaderObject>
{
    public ShardedQueryCommand(BlittableJsonReaderObject query, IndexQueryServerSide indexQuery, bool metadataOnly, bool indexEntriesOnly, string indexName,
        bool canReadFromCache, string raftUniqueRequestId) : base(query, indexQuery, metadataOnly, indexEntriesOnly, indexName, canReadFromCache, raftUniqueRequestId)
    {
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
        {
            // is null only when index doesn't exist
            throw new IndexDoesNotExistException($"Index `{IndexName}` was not found");
        }

        if (fromCache)
            response = HandleCachedResponse(context, response);

        Result = JsonDeserializationClient.QueryResult(response);
    }
}
