using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetBackupResult : AbstractBackupHistoryHandlerProcessor
{
    public BackupDatabaseHandlerProcessorForGetBackupResult([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool ShardNumberRequired => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var shardNumber = GetOptionalShardNumber();
        var taskId = RequestHandler.GetLongQueryString("taskId");
        var createdAt = GetUtcDateTimeQueryString("createdAt", required: true).Value;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var databaseRecord = ReadDatabaseRecord(context, shardNumber))
        {
            var databaseName = shardNumber.HasValue ? ShardHelper.ToShardName(databaseRecord.DatabaseName, shardNumber.Value) : databaseRecord.DatabaseName;
            if (BackupHistoryStorage.TryFindBackupTicks(context, databaseName, taskId, createdAt.Ticks, out var createdAtTicks) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            var backupResult = BackupHistoryStorage.GetBackupResult(context, databaseName, taskId, createdAtTicks);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(BackupResult));
                writer.WriteObject(backupResult);
                writer.WriteEndObject();
            }
        }
    }
}
