using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions.Database;
using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteDatabaseCommand : UpdateDatabaseCommand
    {
        public bool HardDelete;
        public string[] FromNodes;

        public DeleteDatabaseCommand() : base(null)
        {
            ErrorOnDatabaseDoesNotExists = true;
        }

        public DeleteDatabaseCommand(string databaseName) : base(databaseName)
        {
            ErrorOnDatabaseDoesNotExists = true;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var deletionInProgressStatus = HardDelete? DeletionInProgressStatus.HardDelete
                : DeletionInProgressStatus.SoftDelete;
            if (record.DeletionInProgress == null)
            {
                record.DeletionInProgress = new Dictionary<string, DeletionInProgressStatus>();
            }
            if (FromNodes.Length > 0)
            {
                foreach (var node in FromNodes)
                {
                    if (record.Topology.RelevantFor(node) == false)
                    {
                        DatabaseDoesNotExistException.ThrowWithMessage(record.DatabaseName, $"Request to delete database from node '{node}' failed.");
                    }
                    record.Topology.RemoveFromTopology(node);
                    record.Topology.ReplicationFactor--;
                    record.DeletionInProgress[node] = deletionInProgressStatus;
                }
            }
            else
            {
                var allNodes = record.Topology.Members.Select(m => m)
                    .Concat(record.Topology.Promotables.Select(p => p));
                    // TODO: we need to delete databases from watchers too but watcher nodeTag seems to be null
                    //.Concat(record.Topology.Watchers.Select(w => w.NodeTag));

                foreach (var node in allNodes)
                    record.DeletionInProgress[node] = deletionInProgressStatus;

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
            json[nameof(FromNodes)] = FromNodes;
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
        }
    }
}
