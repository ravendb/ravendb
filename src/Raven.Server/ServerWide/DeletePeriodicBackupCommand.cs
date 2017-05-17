using Raven.Client.Documents;
using Raven.Client.Server.PeriodicBackup;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide
{
    public class DeletePeriodicBackupCommand : UpdateDatabaseCommand
    {
        public long BackupTaskId;

        public DeletePeriodicBackupCommand() : base(null)
        {
            // for deserialization
        }

        public DeletePeriodicBackupCommand(long backupTaskId, string databaseName) 
            : base(databaseName)
        {
            BackupTaskId = backupTaskId;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeletePeriodicBackupConfiguration(BackupTaskId);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(BackupTaskId)] = TypeConverter.ToBlittableSupportedType(BackupTaskId);
        }
    }
}
