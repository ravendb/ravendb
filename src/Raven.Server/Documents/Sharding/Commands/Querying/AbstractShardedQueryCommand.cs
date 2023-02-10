using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Http;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Commands.Querying;

public abstract class AbstractShardedQueryCommand<TResult, TParameters> : AbstractQueryCommand<TResult, TParameters>, IRaftCommand
{
    private readonly BlittableJsonReaderObject _query;

    public readonly QueryTimingsScope Scope;

    protected readonly string IndexName;
    protected AbstractShardedQueryCommand(BlittableJsonReaderObject query, IndexQueryBase<TParameters> indexQuery, QueryTimingsScope scope, bool metadataOnly, bool indexEntriesOnly, string indexName,
        bool canReadFromCache, string raftUniqueRequestId) : base(indexQuery, true, metadataOnly, indexEntriesOnly)
    {
        _query = query;
        Scope = scope;
        IndexName = indexName;
        CanReadFromCache = canReadFromCache;
        RaftUniqueRequestId = raftUniqueRequestId;
    }

    public string RaftUniqueRequestId { get; }

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
}
