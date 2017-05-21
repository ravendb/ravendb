using Raven.Client.Documents;
using Raven.Client.Server.PeriodicBackup;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupCommand : UpdateDatabaseCommand
    {
        public PeriodicBackupConfiguration Configuration;

        public UpdatePeriodicBackupCommand() : base(null)
        {
            // for deserialization
        }

        public UpdatePeriodicBackupCommand(PeriodicBackupConfiguration configuration, string databaseName) 
            : base(databaseName)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Configuration.TaskId == null)
                Configuration.TaskId = etag;

            record.AddPeriodicBackupConfiguration(Configuration);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
