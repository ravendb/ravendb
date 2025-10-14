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

        public override bool Disabled => isDisabled();

        private bool isDisabled()
        {
            var disable = false;
            if (Configuration.Default != null)
                disable = Configuration.Default.Disabled;
            if (Configuration.Collections != null && Configuration.Collections.Count > 0)
                disable &= Configuration.Collections.All(x => x.Value.Disabled);
            return disable;
        }
    }
}
