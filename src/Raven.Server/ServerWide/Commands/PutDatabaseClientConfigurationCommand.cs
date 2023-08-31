using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class PutDatabaseClientConfigurationCommand : UpdateDatabaseCommand
    {

        public ClientConfiguration Configuration;

        public PutDatabaseClientConfigurationCommand()
        {
            // for deserialization
        }

        public PutDatabaseClientConfigurationCommand(ClientConfiguration client, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = client;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Client = Configuration;
            record.Client.Etag = etag;
        }
    }
}
