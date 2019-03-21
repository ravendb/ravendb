using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ConfirmRemoveChangeVectorCommand : UpdateDatabaseCommand
    {
        public string Node;

        public ConfirmRemoveChangeVectorCommand():base(null) { } // for de-serialize

        public ConfirmRemoveChangeVectorCommand(string databaseName, string node) : base(databaseName)
        {
            Node = node;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.RemoveChangeVectors == null)
                throw new InvalidOperationException("No change vector removal progress was found.");

            if (record.Topology.RelevantFor(Node) == false)
                throw new InvalidOperationException($"Node {Node} is not relevant for the current topology.");

            if (record.RemoveChangeVectors.Confirmations.Contains(Node) == false)
                record.RemoveChangeVectors.Confirmations.Add(Node);

            
            if (record.RemoveChangeVectors.Confirmations.Count == record.Topology.Count)
            {
                foreach (var node in record.Topology.AllNodes)
                {
                    if (record.RemoveChangeVectors.Confirmations.Contains(node) == false)
                        throw new InvalidOperationException($"Inconsistent confirmations, node {node} is missing, this is likely a bug.");
                }
                record.RemoveChangeVectors.ShouldUpdateGlobal = true;
                record.RemoveChangeVectors.IgnoredList = null;
                record.RemoveChangeVectors.Confirmations = null;
                record.RemoveChangeVectors.Index = 0;
            }
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Node)] = Node;
        }
    }

    public class RemoveChangeVectorCommand : UpdateDatabaseCommand
    {
        public List<string> IgnoreList;

        public RemoveChangeVectorCommand():base(null) { } // for de-serialize

        public RemoveChangeVectorCommand(string databaseName, List<string> ignoreList) : base(databaseName)
        {
            IgnoreList = ignoreList;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.RemoveChangeVectors?.Index > 0)
                throw new InvalidOperationException("Remove change vector command is already in progress.");

            record.RemoveChangeVectors = new RemoveChangeVectors
            {
                Confirmations = new List<string>(),
                IgnoredList = IgnoreList,
                Index = etag
            };

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IgnoreList)] = new DynamicJsonArray(IgnoreList);
        }
    }
}
