using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.Tables;

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

        public override void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
        {
            base.Execute(context, items, index, record, state, out result);

            var currentNodeTag = RachisConsensus.ReadNodeTag(context);
            if (PeriodicBackupStatus.NodeTag == currentNodeTag)
            {
                var status = GetUpdatedValue(index, record, context, null);
                BackupStatusStorage.Insert(context, status.Value, DatabaseName, PeriodicBackupStatus.TaskId);
            }

            // Delete the local status if we are a non-responsible node, and we are overdue on a full backup
            var localStatus = BackupStatusStorage.GetBackupStatus(context, DatabaseName, PeriodicBackupStatus.TaskId);
            if (localStatus == null)
                return;

            var responsibleNode = BackupUtils.GetResponsibleNodeTag(context, DatabaseName, PeriodicBackupStatus.TaskId);
            var config = record.GetPeriodicBackupConfiguration(PeriodicBackupStatus.TaskId);
            if (responsibleNode == null || responsibleNode == currentNodeTag || config.FullBackupFrequency == null)
                return;

            DateTime? nextFullBackup = BackupUtils.GetNextBackupOccurrence(new BackupUtils.NextBackupOccurrenceParameters
            {
                BackupFrequency = config.FullBackupFrequency,
                Configuration = config,
                LastBackupUtc = localStatus.LastFullBackupInternal ?? DateTime.MinValue
            });

            if (nextFullBackup?.ToUniversalTime() < PeriodicBackupStatus.EndTime)
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
