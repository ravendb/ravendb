using System.Linq;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class EditRevisionsConfigurationCommand : UpdateDatabaseRecordFeaturesCommand
    {
        public RevisionsConfiguration Configuration { get; private set; }

        public EditRevisionsConfigurationCommand()
        {
        }

        public EditRevisionsConfigurationCommand(RevisionsConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Revisions = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override bool Disabled => Configuration.Default.Disabled && Configuration.Collections.All(collection => collection.Value.Disabled);
    }
}
