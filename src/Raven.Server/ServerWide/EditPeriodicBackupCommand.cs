using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.PeriodicExport;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide
{
    public class EditPeriodicBackupCommand : UpdateDatabaseCommand
    {
        public PeriodicBackupConfiguration Configuration;
        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.PeriodicBackup = Configuration;
        }

        public EditPeriodicBackupCommand() : base(null)
        {
        }

        public EditPeriodicBackupCommand(PeriodicBackupConfiguration configuration, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.PeriodicBackup = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
