using Raven.Client.Server;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

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

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            // remove the record for the backup
            record.DeletePeriodicBackupConfiguration(TaskId);
            return TaskId.ToString();
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TypeConverter.ToBlittableSupportedType(TaskId);
        }
    }
}
