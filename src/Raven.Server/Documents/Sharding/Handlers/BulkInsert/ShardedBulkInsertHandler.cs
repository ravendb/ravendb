using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.BulkInsert;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class ShardedBulkInsertHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/bulk_insert", "POST")]
    public async Task BulkInsert()
    {
        var operationCancelToken = CreateOperationToken();
        var id = GetLongQueryString("id");
        var skipOverwriteIfUnchanged = GetBoolValueQueryString("skipOverwriteIfUnchanged", required: false) ?? false;

        await using (var processor = new ShardedBulkInsertHandlerProcessor(this, DatabaseContext, id, skipOverwriteIfUnchanged, operationCancelToken.Token))
        {
            await processor.ExecuteAsync();
        }
    }
}
