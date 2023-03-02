using System;
using System.Net.Http;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands.Querying;

public class ShardedQueryCommand : AbstractShardedQueryCommand<QueryResult, BlittableJsonReaderObject>
{
    private readonly IndexQueryServerSide _indexQuery;

    public ShardedQueryCommand(
        string query,
        IndexQueryServerSide indexQuery,
        QueryTimingsScope scope,
        bool metadataOnly,
        bool indexEntriesOnly,
        bool ignoreLimit,
        string indexName,
        bool canReadFromCache,
        string raftUniqueRequestId,
        TimeSpan globalHttpClientTimeout)
        : base(query, indexQuery, scope, metadataOnly, indexEntriesOnly, ignoreLimit, indexName, canReadFromCache, raftUniqueRequestId, globalHttpClientTimeout)
    {
        _indexQuery = indexQuery;
    }

    internal BlittableJsonReaderObject RawResult { get; set; }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        var request = base.CreateRequest(ctx, node, out url);

        if (_indexQuery.AddTimeSeriesNames)
            url += "&addTimeSeriesNames=true";

        return request;
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        using (Scope)
        {
            if (response == null)
            {
                // is null only when index doesn't exist
                throw new IndexDoesNotExistException($"Index `{IndexName}` was not found");
            }

            if (fromCache)
                response = HandleCachedResponse(context, response);

            RawResult = response;

            Result = JsonDeserializationClient.QueryResult(response);

            Scope?.WithBase(Result.Timings);
        }
    }
}
