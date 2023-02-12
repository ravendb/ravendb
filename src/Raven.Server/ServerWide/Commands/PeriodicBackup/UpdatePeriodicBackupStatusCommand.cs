using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
    {
        public PeriodicBackupStatus PeriodicBackupStatus;
        public List<BackupHistoryEntry> BackupHistoryEntries;

        public List<BackupHistoryEntry> CurrentAndTemporarySavedEntries
        {
            get
            {
                var createdAt = PeriodicBackupStatus.IsFull
                    ? PeriodicBackupStatus.LastFullBackup ?? PeriodicBackupStatus.Error.At
                    : PeriodicBackupStatus.LastIncrementalBackup ?? PeriodicBackupStatus.Error.At;

                var entryFromBackupStatus = new BackupHistoryEntry
                {
                    BackupType = PeriodicBackupStatus.BackupType,
                    CreatedAt = createdAt,
                    DatabaseName = DatabaseName,
                    DurationInMs = PeriodicBackupStatus.DurationInMs,
                    Error = PeriodicBackupStatus.Error?.Exception,
                    IsFull = PeriodicBackupStatus.IsFull,
                    NodeTag = PeriodicBackupStatus.NodeTag,
                    LastFullBackup = PeriodicBackupStatus.LastFullBackup,
                    TaskId = PeriodicBackupStatus.TaskId
                };

                return new List<BackupHistoryEntry>(BackupHistoryEntries) { entryFromBackupStatus };
            }
        }

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

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
            json[nameof(BackupHistoryEntries)] = new DynamicJsonArray(BackupHistoryEntries);
        }
    }
}
