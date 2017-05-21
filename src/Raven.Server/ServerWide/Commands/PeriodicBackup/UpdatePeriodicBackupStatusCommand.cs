using Raven.Client.Server;
using Raven.Client.Server.PeriodicBackup;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
    {
        public PeriodicBackupStatus PeriodicBackupStatus;

        private UpdatePeriodicBackupStatusCommand() : base(null)
        {
            // for deserialization
        }

        public UpdatePeriodicBackupStatusCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId()
        {
            return PeriodicBackupStatus.GenerateItemName(DatabaseName, PeriodicBackupStatus.TaskId);
        }

        public override DynamicJsonValue GetUpdatedValue(long index, DatabaseRecord record, BlittableJsonReaderObject existingValue)
        {
            return PeriodicBackupStatus.ToJson();
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
        }
    }
}
