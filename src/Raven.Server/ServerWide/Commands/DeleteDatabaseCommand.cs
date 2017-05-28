using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Exceptions.Database;
using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteDatabaseCommand : UpdateDatabaseCommand
    {
        public bool HardDelete;
        public string FromNode;

        public DeleteDatabaseCommand() : base(null)
        {
        }

        public DeleteDatabaseCommand(string databaseName) : base(databaseName)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var deletionInProgressStatus = HardDelete? DeletionInProgressStatus.HardDelete
                : DeletionInProgressStatus.SoftDelete;
            if (record.DeletionInProgress == null)
            {
                record.DeletionInProgress = new Dictionary<string, DeletionInProgressStatus>();
            }
            if (string.IsNullOrEmpty(FromNode) == false)
            {
                if (record.Topology.RelevantFor(FromNode) == false)
                {
                    throw new DatabaseDoesNotExistException($"We were requested to delete {record.DatabaseName} from {FromNode} but it does not exists in the database record.");
                }
                record.Topology.RemoveFromTopology(FromNode);

                record.DeletionInProgress[FromNode] = deletionInProgressStatus;
            }
            else
            {
                var allNodes = record.Topology.Members.Select(m => m.NodeTag)
                    .Concat(record.Topology.Promotables.Select(p => p.NodeTag));
                    // TODO: we need to delete databases from watchers too but watcher nodeTag seems to be null
                    //.Concat(record.Topology.Watchers.Select(w => w.NodeTag));

                foreach (var node in allNodes)
                    record.DeletionInProgress[node] = deletionInProgressStatus;

                record.Topology = new DatabaseTopology();
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(HardDelete)] = HardDelete;
            json[nameof(FromNode)] = FromNode;
            json[nameof(Etag)] = Etag;
        }
    }
}
