using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public class ShardedUpdateExternalReplicationStateCommand : UpdateValueForDatabaseCommand
    {
        public ShardedExternalReplicationState ReplicationState { get; set; }

        private ShardedUpdateExternalReplicationStateCommand()
        {
            // for deserialization
        }

        public ShardedUpdateExternalReplicationStateCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId()
        {
            return ShardedExternalReplicationState.GenerateShardedItemName(DatabaseName, ReplicationState.SourceDatabaseName, ReplicationState.SourceShardedDatabaseId);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return context.ReadObject(ReplicationState.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ReplicationState)] = ReplicationState.ToJson();
        }
    }
}
