using Raven.Client.ServerWide;
using Raven.Server.Documents.Replication;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateExternalReplicationStateCommand : UpdateValueForDatabaseCommand
    {
        public ExternalReplicationState ExternalReplicationState { get; set; }

        private UpdateExternalReplicationStateCommand() : base(null)
        {
            // for deserialization
        }

        public UpdateExternalReplicationStateCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId()
        {
            return ExternalReplicationState.GenerateItemName(DatabaseName, ExternalReplicationState.TaskId);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            return context.ReadObject(ExternalReplicationState.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ExternalReplicationState)] = ExternalReplicationState.ToJson();
        }
    }
}
