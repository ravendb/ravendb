using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateExternalReplicationStateCommand : UpdateValueForDatabaseCommand
    {
        public ExternalReplicationState ExternalReplicationState { get; set; }

        private UpdateExternalReplicationStateCommand()
        {
            // for deserialization
        }

        public UpdateExternalReplicationStateCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId()
        {
            if (string.IsNullOrEmpty(ExternalReplicationState.SourceDatabaseName))
                return ExternalReplicationState.GenerateItemName(DatabaseName, ExternalReplicationState.TaskId);

            return ExternalReplicationState.GenerateItemName(DatabaseName, ExternalReplicationState.SourceDatabaseName, ExternalReplicationState.SourceDatabaseId);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context,
            BlittableJsonReaderObject existingValue)
        {
            return context.ReadObject(ExternalReplicationState.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ExternalReplicationState)] = ExternalReplicationState.ToJson();
        }
    }
}
