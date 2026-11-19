using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetBackupHistory : AbstractBackupHistoryHandlerProcessor
{
    public BackupDatabaseHandlerProcessorForGetBackupHistory([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool ShardNumberRequired => false;

    protected override async ValueTask WriteResponseAsync(TransactionOperationContext context, RawDatabaseRecord databaseRecord, int? shardNumber)
    {
        var includeIncrementals = RequestHandler.GetBoolValueQueryString("includeIncrementals", required: false) ?? true;
        var requestedTaskId = RequestHandler.GetLongQueryString("taskId", required: false);
        var fullBackupTicks = RequestHandler.GetLongQueryString("fullBackupTicks", required: false);

        if (fullBackupTicks.HasValue && requestedTaskId.HasValue == false)
            throw new ArgumentException($"When requesting specific backup ({nameof(fullBackupTicks)}), `taskId` must be specified", nameof(requestedTaskId));

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(BackupHistory));
            writer.WriteObject(BackupHistoryStorage.GetBackupHistory(context, databaseRecord, includeIncrementals, requestedTaskId, fullBackupTicks));
            writer.WriteEndObject();
        }
    }
}
