using System;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands.Querying;

public class ShardedQueryCommand : AbstractQueryCommand<QueryResult, BlittableJsonReaderObject>
{
    private readonly IndexQueryServerSide _indexQuery;
    private readonly DocumentConventions _conventions;
    private readonly BlittableJsonReaderObject _query;

    public readonly QueryTimingsScope Scope;
    public ShardedQueryCommand(
        DocumentConventions conventions,
        BlittableJsonReaderObject query,
        IndexQueryServerSide indexQuery,
        QueryTimingsScope scope,
        bool metadataOnly,
        bool indexEntriesOnly,
        bool ignoreLimit,
        string indexName,
        bool canReadFromCache,
        string raftUniqueRequestId,
        TimeSpan globalHttpClientTimeout)
        : base(indexQuery, true, metadataOnly, indexEntriesOnly, ignoreLimit, globalHttpClientTimeout)
    {
        _conventions = conventions;
        _query = query;
        _indexQuery = indexQuery;
        Scope = scope;
        IndexName = indexName;
        CanReadFromCache = canReadFromCache;
        RaftUniqueRequestId = raftUniqueRequestId;

        Timeout = null; // we want to use global http timeout (infinite by default in sharding) even if WaitForNonStaleResultsTimeout is specified
    }

    protected readonly string IndexName;

    public string RaftUniqueRequestId { get; }

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
        var queryToWrite = _query.CloneForConcurrentRead(ctx);

        return new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, queryToWrite, CancellationToken), _conventions);
    }
}
