using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditRevisionsForConflictsConfigurationCommand : UpdateDatabaseCommand
    {
        public RevisionsCollectionConfiguration Configuration { get; protected set; }

        public EditRevisionsForConflictsConfigurationCommand()
        {
        }

        public EditRevisionsForConflictsConfigurationCommand(RevisionsCollectionConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.RevisionsForConflicts = Configuration;
            record.ClusterState.LastRevisionsIndex = index;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
