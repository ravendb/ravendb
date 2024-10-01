using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class RevisionsBinConfigurationCommand : UpdateDatabaseCommand
    {
        public RevisionsBinConfiguration Configuration;
        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.RevisionsBin = Configuration;
        }

        public RevisionsBinConfigurationCommand()
        {
        }

        public RevisionsBinConfigurationCommand(RevisionsBinConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RevisionsBin = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
