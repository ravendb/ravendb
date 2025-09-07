using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetBackupHistory : AbstractHandlerProcessor<RequestHandler>
{
    public BackupDatabaseHandlerProcessorForGetBackupHistory([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var databaseName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("database");
        var includeIncrementals = RequestHandler.GetBoolValueQueryString("includeIncrementals", required: false) ?? true;
        var requestedTaskId = RequestHandler.GetLongQueryString("taskId", required: false);
        var fullBackupTicks = RequestHandler.GetLongQueryString("fullBackupTicks", required: false);

        if (fullBackupTicks.HasValue && requestedTaskId.HasValue == false)
            throw new ArgumentException($"When requesting specific backup ({nameof(fullBackupTicks)}), `taskId` must be specified", nameof(requestedTaskId));

        var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(BackupHistory));

                var json = BackupHistoryStorage.GetBackupHistory(context, database.ReadDatabaseRecord(), includeIncrementals, requestedTaskId, fullBackupTicks);
                writer.WriteObject(json);

                writer.WriteEndObject();
            }
        }
    }
}
