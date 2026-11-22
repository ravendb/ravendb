using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide;
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

    protected override async ValueTask WriteResponseAsync(TransactionOperationContext context, RawDatabaseRecord databaseRecord, int? shardNumber)
    {
        var taskId = RequestHandler.GetLongQueryString("taskId");
        var createdAt = GetUtcDateTimeQueryString("createdAt", required: true).Value;
        var databaseName = shardNumber.HasValue ? ShardHelper.ToShardName(databaseRecord.DatabaseName, shardNumber.Value) : databaseRecord.DatabaseName;

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(BackupResult));
            writer.WriteObject(BackupHistoryStorage.GetBackupResult(context, databaseName, taskId, createdAt.Ticks));
            writer.WriteEndObject();
        }
    }
}
