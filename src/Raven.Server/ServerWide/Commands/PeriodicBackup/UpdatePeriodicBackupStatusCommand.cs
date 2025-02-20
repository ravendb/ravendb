using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
    {
        public PeriodicBackupStatus PeriodicBackupStatus;

        // ReSharper disable once UnusedMember.Local
        private UpdatePeriodicBackupStatusCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupStatusCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId()
        {
            return PeriodicBackupStatus.GenerateItemName(DatabaseName, PeriodicBackupStatus.TaskId);
        }

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId()));
        }

        public override void AfterExecute(long index, RawDatabaseRecord record, ClusterOperationContext context, ServerStore serverStore)
        {
            // Update the local status in the relevant node
            if (PeriodicBackupStatus.NodeTag == serverStore.NodeTag)
            {
                var status = GetUpdatedValue(index, record, context, null);
                BackupStatusStorage.InsertBackupStatusBlittable(context, status.Value, DatabaseName, serverStore._env.Base64Id, PeriodicBackupStatus.TaskId);
            }

            // Delete the local status if we are a non responsible node and we are overdue on a full backup
            var localStatus = BackupUtils.GetLocalBackupStatus(serverStore, context, DatabaseName, PeriodicBackupStatus.TaskId);

            if (localStatus == null)
                return;

            var responsibleNode = BackupUtils.GetResponsibleNodeTag(serverStore, DatabaseName, PeriodicBackupStatus.TaskId);
            var config = record.GetPeriodicBackupConfiguration(PeriodicBackupStatus.TaskId);
            if (responsibleNode != null && responsibleNode != serverStore.NodeTag && config.FullBackupFrequency != null)
            {
                var nextFullBackup = BackupUtils.GetNextBackupOccurrence(new BackupUtils.NextBackupOccurrenceParameters()
                {
                    BackupFrequency = config.FullBackupFrequency,
                    Configuration = config,
                    LastBackupUtc = localStatus.LastFullBackupInternal ?? DateTime.MinValue
                });

                var now = DateTime.UtcNow;
                if (nextFullBackup != null && nextFullBackup.Value.ToUniversalTime() < now)
                {
                    // we are overdue for a full backup, we can delete the local status to ensure the next backup will be full
                    // this is in order to free the tombstone cleaners (for both local and compare exchange tombstones) to delete freely for this node

                    BackupStatusStorage.DeleteBackupStatus(context, DatabaseName, serverStore._env.Base64Id, PeriodicBackupStatus.TaskId);
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
        }
    }
}
