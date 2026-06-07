using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class UpdateExternalReplicationStateCommand : UpdateValueForDatabaseCommand
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
            return ExternalReplicationState.GenerateItemName(DatabaseName, ExternalReplicationState.TaskId, ExternalReplicationState.Type);
        }

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
        {
            if (existingValue != null &&
                (ExternalReplicationState.Type == ExternalReplicationState.ReplicationStateType.HubCursor ||
                 ExternalReplicationState.Type == ExternalReplicationState.ReplicationStateType.SinkCursor))
            {
                existingValue.TryGet(nameof(ExternalReplicationState.SourceChangeVector), out string existingCv);
                ExternalReplicationState.SourceChangeVector = ChangeVectorUtils.MergeVectors(
                    ExternalReplicationState.SourceChangeVector, existingCv);

                if (existingCv == ExternalReplicationState.SourceChangeVector)
                    return new UpdatedValue(UpdatedValueActionType.Noop, null);
            }

            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(ExternalReplicationState.ToJson(), GetItemId()));
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ExternalReplicationState)] = ExternalReplicationState.ToJson();
        }
    }
}
