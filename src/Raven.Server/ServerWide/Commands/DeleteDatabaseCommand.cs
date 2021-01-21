using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
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
                        RemoveDatabaseFromSingleNode(record,record.Topology, node,string.Empty, deletionInProgressStatus);
                    }
                    else
                    {
                        for (int i = 0; i < record.Shards.Length; i++)
                        {
                            RemoveDatabaseFromSingleNode(record, record.Shards[i], node, $"${i}", deletionInProgressStatus);

                        }
                    }
                }
            }
            else
            {
                if (record.IsSharded == false)
                {
                    RemoveDatabaseFromAllNodes(record, record.Topology, "", deletionInProgressStatus);
                }
                else
                {
                    for (var i = 0; i < record.Shards.Length; i++)
                    {
                        record.Shards[i] = RemoveDatabaseFromAllNodes(record, record.Shards[i], $"${i}", deletionInProgressStatus);
                    }

                    record.Topology = new DatabaseTopology
                    {
                        Stamp = record.Topology?.Stamp, 
                        ReplicationFactor = 0
                    };
                }
            }
        }

        
        private DatabaseTopology RemoveDatabaseFromAllNodes(DatabaseRecord record,DatabaseTopology topology,string shardIndex, DeletionInProgressStatus deletionInProgressStatus)
        {
            var allNodes = topology.AllNodes.Distinct();

            foreach (var node in allNodes)
            {
                if (ClusterNodes.Contains(node))
                    record.DeletionInProgress[node + shardIndex] = deletionInProgressStatus;
            }

            return new DatabaseTopology {Stamp = record.Topology?.Stamp, ReplicationFactor = 0};
        }

        private void RemoveDatabaseFromSingleNode(DatabaseRecord record, DatabaseTopology topology, string node,string shardIndex, DeletionInProgressStatus deletionInProgressStatus)
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
                record.DeletionInProgress[node + shardIndex] = deletionInProgressStatus;
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
