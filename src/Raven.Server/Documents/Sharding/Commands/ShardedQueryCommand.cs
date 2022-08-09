using System.Net.Http;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands;

public class ShardedQueryCommand : AbstractQueryCommand<BlittableJsonReaderObject, QueryResult>
{
    private readonly BlittableJsonReaderObject _query;
    private readonly string _indexName;

    public ShardedQueryCommand(ShardedDatabaseRequestHandler handler, BlittableJsonReaderObject query, IndexQueryServerSide indexQuery, bool metadataOnly, bool indexEntriesOnly, string indexName) : base(indexQuery, false, metadataOnly, indexEntriesOnly)
    {
        _query = query;
        _indexName = indexName;

        ModifyRequest = r =>
        {
            // TODO arek - this is temporaty solution, we need to refactor that

            r.Headers.TryAddWithoutValidation(Constants.Headers.Sharded, "true");

            var lastKnownClusterTransactionIndex = handler.GetStringFromHeaders(Constants.Headers.LastKnownClusterTransactionIndex);
            if (lastKnownClusterTransactionIndex != null)
                r.Headers.TryAddWithoutValidation(Constants.Headers.LastKnownClusterTransactionIndex, lastKnownClusterTransactionIndex);
        };
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

        Result = JsonDeserializationClient.QueryResult(response);
    }
}
