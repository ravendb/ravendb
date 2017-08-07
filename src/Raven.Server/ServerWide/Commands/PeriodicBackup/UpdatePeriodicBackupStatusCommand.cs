using Raven.Client.ServerWide;
using Raven.Client.ServerWide.PeriodicBackup;
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

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, bool isPassive)
        {
            return context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
        }
    }
}
