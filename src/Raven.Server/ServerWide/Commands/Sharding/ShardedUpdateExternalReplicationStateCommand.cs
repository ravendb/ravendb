using System;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public sealed class ShardedUpdateExternalReplicationStateCommand : UpdateValueForDatabaseCommand
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

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
        {
            if (existingValue == null)
                return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(ReplicationState.ToJson(), GetItemId()));

            var existingState = JsonDeserializationCluster.ShardedExternalReplicationState(existingValue);
            var existingStates = existingState.ReplicationStates;

            foreach (var (shardedSourceName, shardedSourceState) in ReplicationState.ReplicationStates)
            {
                if (existingStates.TryGetValue(shardedSourceName, out var existingShardedSourceState) == false)
                {
                    existingStates[shardedSourceName] = shardedSourceState;
                    continue;
                }

                existingShardedSourceState.LastSourceEtag = Math.Max(existingShardedSourceState.LastSourceEtag, shardedSourceState.LastSourceEtag);
                existingShardedSourceState.LastSourceChangeVector = ChangeVectorUtils.MergeVectors(existingShardedSourceState.LastSourceChangeVector, shardedSourceState.LastSourceChangeVector);

                foreach (var (shardedDestinationName, shardedDestinationState) in shardedSourceState.DestinationStates)
                {
                    if (existingShardedSourceState.DestinationStates.TryGetValue(shardedDestinationName, out var existingShardedDestinationState) == false)
                    {
                        existingShardedSourceState.DestinationStates[shardedDestinationName] = shardedDestinationState;
                        continue;
                    }

                    existingShardedDestinationState.LastSentEtag = Math.Max(existingShardedDestinationState.LastSentEtag, shardedDestinationState.LastSentEtag);
                    existingShardedDestinationState.DestinationChangeVector = ChangeVectorUtils.MergeVectors(existingShardedDestinationState.DestinationChangeVector, shardedDestinationState.DestinationChangeVector);
                }
            }

            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(existingState.ToJson(), GetItemId()));
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ReplicationState)] = ReplicationState.ToJson();
        }
    }
}
