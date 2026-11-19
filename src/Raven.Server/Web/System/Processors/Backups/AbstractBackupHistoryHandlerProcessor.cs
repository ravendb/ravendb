using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal abstract class AbstractBackupHistoryHandlerProcessor : AbstractServerHandlerProxyReadProcessor<BlittableJsonReaderObject>
{
    private string _databaseName;

    protected AbstractBackupHistoryHandlerProcessor([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected abstract bool ShardNumberRequired { get; }

    public override async ValueTask ExecuteAsync()
    {
        _databaseName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("database");
        if (await RequestHandler.CanAccessDatabaseAsync(_databaseName, requireAdmin: false, requireWrite: false) == false)
            return;

        await base.ExecuteAsync();
    }

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var shardNumber = RequestHandler.GetIntValueQueryString(Constants.QueryString.ShardNumber, required: false);

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, _databaseName))
        {
            if (databaseRecord == null)
                DatabaseDoesNotExistException.Throw(_databaseName);

            AssertShardNumber(databaseRecord, shardNumber);

            await WriteResponseAsync(context, databaseRecord, shardNumber);
        }
    }

    protected override ValueTask<RavenCommand<BlittableJsonReaderObject>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        var request = RequestHandler.HttpContext.Request;
        return ValueTask.FromResult<RavenCommand<BlittableJsonReaderObject>>(new GetBackupHistoryFromNodeCommand(nodeTag, $"{request.Path}{request.QueryString}"));
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<BlittableJsonReaderObject> command, JsonOperationContext context, OperationCancelToken token) =>
        ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);

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
