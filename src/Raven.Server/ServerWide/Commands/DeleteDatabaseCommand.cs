using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteDatabaseCommand : UpdateDatabaseCommand
    {
        public string[] ClusterNodes;
        public bool HardDelete;
        public string[] FromNodes;
        public bool UpdateReplicationFactor = true;

        public DeleteDatabaseCommand()
        {
        }

        public DeleteDatabaseCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            ErrorOnDatabaseDoesNotExists = false;
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
                        RemoveDatabaseFromSingleNode(record, record.Topology, node, shardNumber: null, deletionInProgressStatus);
                    }
                    else
                    {
                        for (int i = 0; i < record.Sharding.Shards.Length; i++)
                        {
                            RemoveDatabaseFromSingleNode(record, record.Sharding.Shards[i], node, i, deletionInProgressStatus); //TODO stav: might need to remove
                        }
                    }
                }
            }
            else
            {
                if (record.IsSharded == false)
                {
                    RemoveDatabaseFromAllNodes(record, record.Topology, shardNumber: null, deletionInProgressStatus);
                }
                else
                {
                    for (var i = 0; i < record.Sharding.Shards.Length; i++)
                    {
                        record.Sharding.Shards[i] = RemoveDatabaseFromAllNodes(record, record.Sharding.Shards[i], i, deletionInProgressStatus);
                    }
                }
            }
        }

        
        private DatabaseTopology RemoveDatabaseFromAllNodes(DatabaseRecord record, DatabaseTopology topology, int? shardNumber, DeletionInProgressStatus deletionInProgressStatus)
        {
            var allNodes = topology.AllNodes.Distinct();

            foreach (var node in allNodes)
            {
                if (ClusterNodes.Contains(node))
                    record.DeletionInProgress[DatabaseTopology.GetKeyForDeletionInProgress(node, shardNumber)] = deletionInProgressStatus;
            }

            return new DatabaseTopology {Stamp = record.Topology?.Stamp, ReplicationFactor = 0};
        }

        private void RemoveDatabaseFromSingleNode(DatabaseRecord record, DatabaseTopology topology, string node, int? shardNumber, DeletionInProgressStatus deletionInProgressStatus)
        {
            if (topology.RelevantFor(node) == false)
            {
                DatabaseDoesNotExistException.ThrowWithMessage(record.DatabaseName, $"Request to delete database from node '{node}' failed.");
            }

            // rehabs will be removed only once the replication sent all the documents to the mentor
            if (topology.Rehabs.Contains(node) == false)
                topology.RemoveFromTopology(node);

            if (UpdateReplicationFactor)
            {
                topology.ReplicationFactor--;
            }

            if (ClusterNodes.Contains(node))
                record.DeletionInProgress[DatabaseTopology.GetKeyForDeletionInProgress(node, shardNumber)] = deletionInProgressStatus;
        }
        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(HardDelete)] = HardDelete;
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
            json[nameof(UpdateReplicationFactor)] = UpdateReplicationFactor;
            if (FromNodes != null)
            {
              json[nameof(FromNodes)] = new DynamicJsonArray(FromNodes);
            }
        }
    }
}
