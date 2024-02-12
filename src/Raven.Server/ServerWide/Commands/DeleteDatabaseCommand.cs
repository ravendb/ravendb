using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
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

        public static string GenerateUniqueRequestId(string dbName, IEnumerable<string> fromNodes, string guid)
        {
            return $"DeleteDatabase/{dbName}/{string.Join('_', fromNodes)}/{guid}";
        }

        public override void Initialize(ServerStore serverStore, ClusterOperationContext context)
        {
            ClusterNodes = serverStore.GetClusterTopology(context).AllNodes.Keys.ToArray();
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            VerifyDeletion(record);

            var deletionInProgressStatus = HardDelete ? DeletionInProgressStatus.HardDelete
                : DeletionInProgressStatus.SoftDelete;

            record.DeletionInProgress ??= new Dictionary<string, DeletionInProgressStatus>();

            if (FromNodes != null && FromNodes.Length > 0) 
            {
                foreach (var node in FromNodes)
                {
                    if (record.Topology.RelevantFor(node) == false)
                    {
                        DatabaseDoesNotExistException.ThrowWithMessage(record.DatabaseName, $"Request to delete database from node '{node}' failed.");
                    }

                    // rehabs will be removed only once the replication sent all the documents to the mentor
                    if (record.Topology.Rehabs.Contains(node) == false) 
                        record.Topology.RemoveFromTopology(node);

                    if (UpdateReplicationFactor)
                    {
                        record.Topology.ReplicationFactor--;
                    }
                    if (ClusterNodes.Contains(node))
                        record.DeletionInProgress[node] = deletionInProgressStatus;
                }
            }
            else
            {
                var allNodes = record.Topology.Members.Select(m => m)
                    .Concat(record.Topology.Promotables.Select(p => p))
                    .Concat(record.Topology.Rehabs.Select(r => r));

                foreach (var node in allNodes)
                {
                    if (ClusterNodes.Contains(node))
                        record.DeletionInProgress[node] = deletionInProgressStatus;
                }

                record.Topology = new DatabaseTopology
                {
                    Stamp = record.Topology.Stamp,
                    ReplicationFactor = 0
                };
            }

            record.Topology.Stamp.Index = etag;
        }

        private void VerifyDeletion(DatabaseRecord record)
        {
            var databaseName = record.DatabaseName;
            var topology = record.Topology;
            foreach (var node in FromNodes)
            {
                if (topology.RelevantFor(node) == false)
                {
                    throw new RachisInvalidOperationException($"Database '{databaseName}' doesn't reside on node '{node}' so it can't be deleted from it");
                }
            }
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
