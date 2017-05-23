using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            if (string.IsNullOrEmpty(FromNode) == false)
            {
                //TODO: maybe expose a way to issue errors when applying commands so we can atleast raise alerts
                if (record.Topology.RelevantFor(FromNode) == false)
                {
                    return;
                }
                record.Topology.RemoveFromTopology(FromNode);

                record.DeletionInProgress[FromNode] = deletionInProgressStatus;
            }
            else
            {
                var allNodes = record.Topology.Members.Select(m => m.NodeTag)
                    .Concat(record.Topology.Promotables.Select(p => p.NodeTag))
                    .Concat(record.Topology.Watchers.Select(w => w.NodeTag));

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
