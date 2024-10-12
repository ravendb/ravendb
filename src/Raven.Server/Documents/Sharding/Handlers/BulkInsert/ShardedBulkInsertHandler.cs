using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.BulkInsert;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public sealed class ShardedBulkInsertHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/bulk_insert", "POST")]
    public async Task BulkInsert()
    {
        var operationCancelToken = CreateHttpRequestBoundOperationToken();
        var id = GetLongQueryString("id");
        var skipOverwriteIfUnchanged = GetBoolValueQueryString("skipOverwriteIfUnchanged", required: false) ?? false;

        await using (var processor = new ShardedBulkInsertHandlerProcessor(this, DatabaseContext, id, skipOverwriteIfUnchanged, operationCancelToken.Token))
        {
            if (DatabaseContext.ForTestingPurposes?.BulkInsertStreamWriteTimeout > 0)
                processor.ForTestingPurposesOnly().BulkInsert_StreamReadTimeout = DatabaseContext.ForTestingPurposes.BulkInsertStreamWriteTimeout;

            await processor.ExecuteAsync();
        }
    }
}
