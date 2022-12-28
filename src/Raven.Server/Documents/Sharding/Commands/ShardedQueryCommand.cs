using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Queries;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Commands;

public class ShardedQueryCommand : AbstractQueryCommand<BlittableJsonReaderObject, QueryResult>, IRaftCommand
{
    private readonly BlittableJsonReaderObject _query;
    private readonly string _indexName;

    public ShardedQueryCommand(BlittableJsonReaderObject query, IndexQueryServerSide indexQuery, bool metadataOnly, bool indexEntriesOnly, string indexName,
        bool canReadFromCache, string raftUniqueRequestId) : base(indexQuery, true, metadataOnly, indexEntriesOnly)
    {
        _query = query;
        _indexName = indexName;
        CanReadFromCache = canReadFromCache;
        RaftUniqueRequestId = raftUniqueRequestId;
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
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "let's create a server-side query class here and use same code as for QueryCommand");
        return new StringContent(_query.ToString(), Encoding.UTF8, "application/json");
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

    public string RaftUniqueRequestId { get; }
}
