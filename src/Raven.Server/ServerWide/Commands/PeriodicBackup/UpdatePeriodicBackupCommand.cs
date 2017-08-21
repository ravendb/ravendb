using Raven.Client.ServerWide;
using Raven.Client.ServerWide.PeriodicBackup;
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

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Configuration.TaskId == 0)
            {
                // this is a new backup configuration
                Configuration.TaskId = etag;
            }

            record.AddPeriodicBackupConfiguration(Configuration);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
