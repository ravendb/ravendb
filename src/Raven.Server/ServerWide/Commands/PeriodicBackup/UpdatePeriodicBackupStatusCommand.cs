using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public sealed class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
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

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId()));
        }

        public override void AfterExecute(long index, RawDatabaseRecord record, ClusterOperationContext context, ServerStore serverStore)
        {
            if (PeriodicBackupStatus.NodeTag == serverStore.NodeTag)
            {
                var status = GetUpdatedValue(index, record, context, null);
                BackupStatusStorage.Insert(context, status.Value, DatabaseName, PeriodicBackupStatus.TaskId);
            }

            // Delete the local status if we are a non-responsible node, and we are overdue on a full backup
            var localStatus = serverStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(context, DatabaseName, PeriodicBackupStatus.TaskId);
            if (localStatus == null)
                return;

            var responsibleNode = BackupUtils.GetResponsibleNodeTag(serverStore, DatabaseName, PeriodicBackupStatus.TaskId);
            var config = record.GetPeriodicBackupConfiguration(PeriodicBackupStatus.TaskId);
            if (responsibleNode == null || responsibleNode == serverStore.NodeTag || config.FullBackupFrequency == null)
                return;

            DateTime? nextFullBackup = BackupUtils.GetNextBackupOccurrence(new BackupUtils.NextBackupOccurrenceParameters
            {
                BackupFrequency = config.FullBackupFrequency,
                Configuration = config,
                LastBackupUtc = localStatus.LastFullBackupInternal ?? DateTime.MinValue
            });

            if (nextFullBackup?.ToUniversalTime() < DateTime.UtcNow)
            {
                // We're overdue for a full backup. We can delete the local status to ensure the next backup is full.
                // This is to allow the tombstone cleaner to freely delete tombstones for this node.
                BackupStatusStorage.Delete(context, DatabaseName, PeriodicBackupStatus.TaskId);
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
        }
    }
}
