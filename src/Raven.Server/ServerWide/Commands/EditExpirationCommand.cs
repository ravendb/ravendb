using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Expiration;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditExpirationCommand : UpdateDatabaseCommand
    {
        public ExpirationConfiguration Configuration;
        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.Expiration = Configuration;
        }

        public EditExpirationCommand() : base(null)
        {
        }

        public EditExpirationCommand(ExpirationConfiguration configuration, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Expiration = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}