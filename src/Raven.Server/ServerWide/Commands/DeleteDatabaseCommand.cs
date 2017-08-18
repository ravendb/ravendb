using System;
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
        public string[] _clusterNodes;
        public bool HardDelete;
        public string[] FromNodes;
        public bool UpdateReplicationFactor = true;

        public DeleteDatabaseCommand() : base(null)
        {
            ErrorOnDatabaseDoesNotExists = true;
        }

        public DeleteDatabaseCommand(string databaseName) : base(databaseName)
        {
            ErrorOnDatabaseDoesNotExists = true;
        }

        public override void Initialize(ServerStore serverStore, TransactionOperationContext context)
        {
            _clusterNodes = serverStore.GetClusterTopology(context).AllNodes.Keys.ToArray();
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
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
                    if (record.Topology.RelevantFor(node) == false)
                    {
                        DatabaseDoesNotExistException.ThrowWithMessage(record.DatabaseName, $"Request to delete database from node '{node}' failed.");
                    }
                    record.Topology.RemoveFromTopology(node);
                    if (UpdateReplicationFactor)
                    {
                        record.Topology.ReplicationFactor--;
                    }
                    if(_clusterNodes.Contains(node))
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
                    if (_clusterNodes.Contains(node))
                        record.DeletionInProgress[node] = deletionInProgressStatus;
                }

                record.Topology.ReplicationFactor = 0;
                record.Topology = new DatabaseTopology
                {
                    Stamp = record.Topology.Stamp
                };
            }
            return null;
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
