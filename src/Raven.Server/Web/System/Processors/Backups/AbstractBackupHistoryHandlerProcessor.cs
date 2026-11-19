using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.System.Processors.Backups;

internal abstract class AbstractBackupHistoryHandlerProcessor : AbstractHandlerProcessor<RequestHandler>
{
    protected AbstractBackupHistoryHandlerProcessor([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract bool ShardNumberRequired { get; }

    public override async ValueTask ExecuteAsync()
    {
        var databaseName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("database");
        if (await RequestHandler.CanAccessDatabaseAsync(databaseName, requireAdmin: false, requireWrite: false) == false)
            return;

        var shardNumber = RequestHandler.GetIntValueQueryString("shardNumber", required: false);

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
        {
            if (databaseRecord == null)
                DatabaseDoesNotExistException.Throw(databaseName);

            AssertShardNumber(databaseRecord, shardNumber);

            await WriteResponseAsync(context, databaseRecord, shardNumber);
        }
    }

    protected abstract ValueTask WriteResponseAsync(TransactionOperationContext context, RawDatabaseRecord databaseRecord, int? shardNumber);

    private void AssertShardNumber(RawDatabaseRecord databaseRecord, int? shardNumber)
    {
        if (databaseRecord.IsSharded == false)
        {
            if (shardNumber.HasValue)
                throw new BadRequestException($"Database '{databaseRecord.DatabaseName}' is not sharded, so 'shardNumber' cannot be specified");

            return;
        }

        if (shardNumber.HasValue == false)
        {
            if (ShardNumberRequired)
                throw new BadRequestException($"Database '{databaseRecord.DatabaseName}' is sharded, so 'shardNumber' must be specified");

            return;
        }

        if (databaseRecord.Sharding.Shards.ContainsKey(shardNumber.Value) == false)
            throw new BadRequestException($"Database '{databaseRecord.DatabaseName}' doesn't have shard {shardNumber.Value}");
    }
}
