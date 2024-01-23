using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class DeleteDatabaseCommand : UpdateDatabaseCommand
    {
        public string[] ClusterNodes;
        public bool HardDelete;
        public string[] FromNodes;
        public bool UpdateReplicationFactor = true;
        public int? ShardNumber = null;

        public DeleteDatabaseCommand()
        {
        }

        public DeleteDatabaseCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            ErrorOnDatabaseDoesNotExists = false;

            if (ShardHelper.TryGetShardNumberAndDatabaseName(DatabaseName, out DatabaseName, out var shard))
                ShardNumber = shard;
        }

        public override void Initialize(ServerStore serverStore, ClusterOperationContext context)
        {
            ClusterNodes = serverStore.GetClusterTopology(context).AllNodes.Keys.ToArray();
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var deletionInProgressStatus = HardDelete ? DeletionInProgressStatus.HardDelete
                : DeletionInProgressStatus.SoftDelete;

            if (record.DeletionInProgress == null)
            {
                record.DeletionInProgress = new Dictionary<string, DeletionInProgressStatus>();
            }

            if (FromNodes != null && FromNodes.Length > 0)
            {
                foreach (var node in FromNodes)
                {
                    if (record.IsSharded == false)
                    {
                        RemoveDatabaseFromSingleNode(record, record.Topology, node, shardNumber: null, deletionInProgressStatus, etag);
                    }
                    else
                    {
                        if (ShardNumber.HasValue)
                        {
                            if (record.Sharding.Shards.TryGetValue(ShardNumber.Value, out var topology) == false)
                                throw new RachisApplyException($"The requested shard '{ShardNumber}' doesn't exists in '{record.DatabaseName}'");

                            if (topology.ReplicationFactor == 1)
                            {
                                if (record.Sharding.DoesShardHaveBuckets(ShardNumber.Value))
                                    throw new RachisApplyException(
                                        $"Database {DatabaseName} cannot be deleted because it is the last copy of shard {ShardNumber.Value} and it still contains buckets.");
                                if (record.Sharding.DoesShardHavePrefixes(ShardNumber.Value))
                                    throw new InvalidOperationException(
                                        $"Database {DatabaseName} cannot be deleted because it is the last copy of shard {ShardNumber.Value} and there are prefixes settings for this shard.");
                            }

                            RemoveDatabaseFromSingleNode(record, record.Sharding.Shards[ShardNumber.Value], node, shardNumber: ShardNumber, deletionInProgressStatus, etag);
                            return;
                        }
                        
                        throw new RachisApplyException($"Deleting entire sharded database {DatabaseName} from a specific node is not allowed.");
                    }
                }
            }
            else
            {
                if (record.IsSharded == false)
                {
                    RemoveDatabaseFromAllNodes(record, record.Topology, shardNumber: null, deletionInProgressStatus, etag);
                }
                else
                {
                    if (ShardNumber.HasValue)
                        throw new RachisApplyException(
                            $"Deleting an entire shard group (shard {ShardNumber.Value}) from the database is not allowed. Use {nameof(DeleteDatabaseCommand)} instead to delete all of this shard's databases.");

                    foreach (var (shardNumber, topology) in record.Sharding.Shards)
                    {
                        record.Sharding.Shards[shardNumber] = RemoveDatabaseFromAllNodes(record, topology, shardNumber, deletionInProgressStatus, etag);
                    }
                }
            }
        }

        private DatabaseTopology RemoveDatabaseFromAllNodes(DatabaseRecord record, DatabaseTopology topology, int? shardNumber, DeletionInProgressStatus deletionInProgressStatus, long etag)
        {
            var allNodes = topology.AllNodes.Distinct();

            foreach (var node in allNodes)
            {
                if (ClusterNodes.Contains(node))
                    record.DeletionInProgress[DatabaseRecord.GetKeyForDeletionInProgress(node, shardNumber)] = deletionInProgressStatus;
            }
            
            var newTopology = new DatabaseTopology { ReplicationFactor = 0 };
            SetLeaderStampForTopology(newTopology, etag);
            return newTopology;
        }

        private void RemoveDatabaseFromSingleNode(DatabaseRecord record, DatabaseTopology topology, string node, int? shardNumber, DeletionInProgressStatus deletionInProgressStatus, long etag)
        {
            if (topology.RelevantFor(node) == false)
            {
                DatabaseDoesNotExistException.ThrowWithMessage(ShardNumber.HasValue ? ShardHelper.ToShardName(record.DatabaseName, ShardNumber.Value) : record.DatabaseName,
                    $"Request to delete database from node '{node}' failed because it does not exist on this node.");
            }

            // rehabs will be removed only once the replication sent all the documents to the mentor
            if (topology.Rehabs.Contains(node) == false)
                topology.RemoveFromTopology(node);

            if (UpdateReplicationFactor)
            {
                topology.ReplicationFactor--;
            }

            if (ClusterNodes.Contains(node))
                record.DeletionInProgress[DatabaseRecord.GetKeyForDeletionInProgress(node, shardNumber)] = deletionInProgressStatus;

            SetLeaderStampForTopology(topology, etag);
        }
        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(HardDelete)] = HardDelete;
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
            json[nameof(UpdateReplicationFactor)] = UpdateReplicationFactor;
            json[nameof(ShardNumber)] = ShardNumber;
            if (FromNodes != null)
            {
                json[nameof(FromNodes)] = new DynamicJsonArray(FromNodes);
            }
        }
    }
}
