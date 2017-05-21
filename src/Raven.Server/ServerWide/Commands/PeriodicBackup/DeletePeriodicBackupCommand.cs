using Raven.Client.Server;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class DeletePeriodicBackupCommand : UpdateDatabaseCommand
    {
        public long TaskId;

        public DeletePeriodicBackupCommand() : base(null)
        {
            // for deserialization
        }

        public DeletePeriodicBackupCommand(long taskId, string databaseName) 
            : base(databaseName)
        {
            TaskId = taskId;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeletePeriodicBackupConfiguration(TaskId);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TypeConverter.ToBlittableSupportedType(TaskId);
        }
    }
}
