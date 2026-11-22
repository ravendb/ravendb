using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
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
    protected string DatabaseName { get; private set; }

    protected AbstractBackupHistoryHandlerProcessor([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected abstract bool ShardNumberRequired { get; }

    public override async ValueTask ExecuteAsync()
    {
        DatabaseName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("database");
        if (await RequestHandler.CanAccessDatabaseAsync(DatabaseName, requireAdmin: false, requireWrite: false) == false)
            return;

        await base.ExecuteAsync();
    }

    protected override ValueTask<RavenCommand<BlittableJsonReaderObject>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        var request = RequestHandler.HttpContext.Request;
        return ValueTask.FromResult<RavenCommand<BlittableJsonReaderObject>>(new GetBackupHistoryFromNodeCommand(nodeTag, $"{request.Path}{request.QueryString}"));
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<BlittableJsonReaderObject> command, JsonOperationContext context, OperationCancelToken token) =>
        ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);

    protected int? GetOptionalShardNumber() => TryGetShardNumber(out var shardNumber) ? shardNumber : null;

    protected RawDatabaseRecord ReadDatabaseRecord(TransactionOperationContext context, int? shardNumber)
    {
        var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, DatabaseName);
        if (databaseRecord == null)
            DatabaseDoesNotExistException.Throw(DatabaseName);

        try
        {
            AssertShardNumber(databaseRecord, shardNumber);
        }
        catch
        {
            databaseRecord.Dispose();
            throw;
        }

        return databaseRecord;
    }

    protected DateTime? GetUtcDateTimeQueryString(string name, bool required)
    {
        DateTime? value;
        try
        {
            value = RequestHandler.GetDateTimeQueryString(name, required);
        }
        catch (ArgumentException e)
        {
            throw new BadRequestException(e.Message);
        }

        return value?.Kind == DateTimeKind.Local ? value.Value.ToUniversalTime() : value;
    }

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
