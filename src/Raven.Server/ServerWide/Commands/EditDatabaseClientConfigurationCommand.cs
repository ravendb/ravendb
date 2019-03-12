using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditDatabaseClientConfigurationCommand : UpdateDatabaseCommand
    {
        public ClientConfiguration Configuration { get; set; }

        public EditDatabaseClientConfigurationCommand() : base(null)
        {
        }

        public EditDatabaseClientConfigurationCommand(ClientConfiguration configuration, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Client = Configuration;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
