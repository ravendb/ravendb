using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Versioning;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditVersioningCommand : UpdateDatabaseCommand
    {
        public VersioningConfiguration Configuration;

        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.Versioning = Configuration;
        }

        public EditVersioningCommand() : base(null)
        {
        }

        public EditVersioningCommand(VersioningConfiguration configuration, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Versioning = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}