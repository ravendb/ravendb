using System.Diagnostics;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands;

public class ShardedQueryCommand : AbstractQueryCommand<BlittableJsonReaderObject, QueryResult>
{
    private readonly BlittableJsonReaderObject _query;
    private readonly string _indexName;

    public ShardedQueryCommand(BlittableJsonReaderObject query, IndexQueryServerSide indexQuery, bool metadataOnly, bool indexEntriesOnly, string indexName, bool canReadFromCache) : base(indexQuery, true, metadataOnly, indexEntriesOnly)
    {
        _query = query;
        _indexName = indexName;
        CanReadFromCache = canReadFromCache;
    }

    protected override ulong GetQueryHash(JsonOperationContext ctx)
    {
        using (var hasher = new HashCalculator(ctx))
        {
            hasher.Write(_query);

            return hasher.GetHash();
        }
    }

    protected override HttpContent GetContent(JsonOperationContext ctx)
    {
        var queryToSend = _query.Clone(ctx);

        return new BlittableJsonContent(async stream => await queryToSend.WriteJsonToAsync(stream));
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
        {
            // is null only when index doesn't exist
            throw new IndexDoesNotExistException($"Index `{_indexName}` was not found");
        }

        if (fromCache) 
            response = HandleCachedResponse(context, response);

        Result = JsonDeserializationClient.QueryResult(response);
    }
}
