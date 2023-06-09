using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.System;

internal class BackupHistoryHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/backup-history-details", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetBackupHistoryDetails()
    {
        var taskId = GetLongQueryString("taskId");
        var createdAtTicksAsId = GetLongQueryString("id");

        using (Database.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            var key = $"values/{Database.Name}/backup-history/{BackupHistoryItemType.Details}/{taskId}/{ServerStore.NodeTag}/{createdAtTicksAsId}";

            writer.WriteStartObject();
            writer.WritePropertyName(nameof(BackupHistoryItemType.Details));

            var details = Database.ConfigurationStorage.BackupHistoryStorage.GetBackupHistoryDetails(key, context);
            writer.WriteObject(details);

            writer.WriteEndObject();
        }
    }

    [RavenAction("/databases/*/backup-history", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetBackupHistory()
    {
        var requestedTaskId = GetLongQueryString("taskId", required: false);
        var requestedNodeTag = GetStringValuesQueryString("nodeTag", required: false);

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
        using (serverStoreContext.OpenReadTransaction())
        using (Database.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationContext))
        using (configurationContext.OpenReadTransaction())
        await using (var jsonTextWriter = new AsyncBlittableJsonTextWriter(serverStoreContext, ResponseBodyStream()))
        {
            jsonTextWriter.WriteStartObject();
            jsonTextWriter.WritePropertyName(nameof(BackupHistory));
            jsonTextWriter.WriteStartArray();

            var backupHistoryWriter = new BackupHistoryWriter(requestedNodeTag, requestedTaskId, ServerStore.NodeTag);

            backupHistoryWriter.GetBackupHistoryEntriesFromCluster(Database, ServerStore, serverStoreContext);
            backupHistoryWriter.GetBackupHistoryEntriesFromTemporaryStorage(Database, serverStoreContext, configurationContext);

            backupHistoryWriter.ProcessClusterStorageEntries(jsonTextWriter, serverStoreContext);
            backupHistoryWriter.ProcessTemporaryStorageEntries(jsonTextWriter, serverStoreContext);

            jsonTextWriter.WriteEndArray();
            jsonTextWriter.WriteEndObject();
        }
    }
}

