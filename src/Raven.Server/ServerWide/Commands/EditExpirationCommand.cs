using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.ServerWide;
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

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Expiration = Configuration;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}