using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System;

internal class BackupHistoryHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/backup/result", "GET", AuthorizationStatus.DatabaseAdmin)]
    public async Task GetBackupResult()
    {
        var taskId = GetLongQueryString("taskId");
        var createdAtTicksAsId = GetLongQueryString("id");

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(BackupResult));

            var json = BackupHistoryStorage.GetBackupResult(context, Database.Name, taskId, createdAtTicksAsId);
            writer.WriteObject(json);

            writer.WriteEndObject();
        }
    }

    [RavenAction("/databases/*/admin/backup/history", "GET", AuthorizationStatus.DatabaseAdmin)]
    public async Task GetBackupHistory()
    {
        var includeIncrementals = GetBoolValueQueryString("includeIncrementals", required: false) ?? true;
        var requestedTaskId = GetLongQueryString("taskId", required: false);
        var fullBackupTicks = GetLongQueryString("fullBackupTicks", required: false);

        if (fullBackupTicks.HasValue && requestedTaskId.HasValue == false)
            throw new ArgumentException($"When requesting specific backup ({nameof(fullBackupTicks)}), taskId must be specified", nameof(requestedTaskId));

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(BackupHistory));

                var json = BackupHistoryStorage.GetBackupHistory(context, Database.ReadDatabaseRecord(), includeIncrementals, requestedTaskId, fullBackupTicks);
                writer.WriteObject(json);

                writer.WriteEndObject();
            }
        }
    }
}
