using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class PutRollingIndexCommand : UpdateDatabaseCommand
    {
        public string IndexName { get; set; }
        public string FinishedNodeTag { get; set; }
        public DateTime? FinishedAt { get; set; }
        public bool CompleteAll { get; set; }

        public PutRollingIndexCommand()
        {
            // for deserialization
        }

        public PutRollingIndexCommand(string databaseName, string indexName, string finishedNodeTag, DateTime? finishedAt, string uniqueRequestId) 
            : base(databaseName, uniqueRequestId)
        {
            IndexName = indexName;
            FinishedNodeTag = finishedNodeTag;
            FinishedAt = finishedAt;
        }

        public PutRollingIndexCommand(string databaseName, string indexName, DateTime? finishedAt, string uniqueRequestId) 
            : base(databaseName, uniqueRequestId)
        {
            IndexName = indexName;
            CompleteAll = true;
            FinishedAt = finishedAt;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.RollingIndexes == null)
            {
                return;
            }

            if (record.RollingIndexes.TryGetValue(IndexName, out var rollingIndex ) == false)
            {
                return; // was already removed
            }

            MaybeAddNewNodesToRollingDeployment(record, rollingIndex.ActiveDeployments);

            if (CompleteAll)
            {
                foreach (var nodeTag in record.Topology.AllNodes)
                {
                    FinishOneNode(record, nodeTag, rollingIndex);
                }
                return;
            }
         
            FinishOneNode(record, FinishedNodeTag, rollingIndex);
        }

        private void FinishOneNode(DatabaseRecord record, string finishedNodeTag, RollingIndex rollingIndex)
        {
            if (string.IsNullOrEmpty(finishedNodeTag))
                return;

            if (rollingIndex.ActiveDeployments.TryGetValue(finishedNodeTag, out var rollingDeployment) == false)
                return;

            if (rollingDeployment.State == RollingIndexState.Done)
                return;

            rollingDeployment.State = RollingIndexState.Done;
            rollingDeployment.FinishedAt = FinishedAt;

            // If we are done and there is already a running node, there is nothing to do. That running node will continue from here.
            if (rollingIndex.ActiveDeployments.Any(node => node.Value.State == RollingIndexState.Running))
            {
                return;
            }

            var chosenNode = ChooseNextNode(record, rollingIndex.ActiveDeployments);
            if (chosenNode == null)
            {
                if (rollingIndex.ActiveDeployments.All(x => x.Value.State == RollingIndexState.Done))
                {
                    if (record.IndexesHistory.TryGetValue(IndexName, out var indexHistoryEntries))
                    {
                        if (indexHistoryEntries.Count > 0)
                            indexHistoryEntries[0].RollingDeployment = rollingIndex.ActiveDeployments;
                    }

                    record.RollingIndexes.Remove(IndexName);
                }

                return;
            }

            rollingIndex.ActiveDeployments[chosenNode].State = RollingIndexState.Running;
            rollingIndex.ActiveDeployments[chosenNode].StartedAt = FinishedAt;
        }

        private static void MaybeAddNewNodesToRollingDeployment(DatabaseRecord record, Dictionary<string, RollingIndexDeployment> rollingIndex)
        {
            var allNodes = record.Topology.AllNodes;

            foreach (var node in allNodes)
            {
                rollingIndex.TryAdd(node, new RollingIndexDeployment
                {
                    State = RollingIndexState.Pending, 
                    CreatedAt = SystemTime.UtcNow
                });
            }
        }

        private string ChooseNextNode(DatabaseRecord record, Dictionary<string, RollingIndexDeployment> rollingIndex)
        {
            var members = record.Topology.Members;
            for (var i = members.Count - 1; i >= 0; i--)
            {
                var node = members[i];
                if (IsNextNode(node)) 
                    return node;
            }

            var promotables = record.Topology.Promotables;
            for (var i = promotables.Count - 1; i >= 0; i--)
            {
                var node = promotables[i];
                if (IsNextNode(node)) 
                    return node;
            }

            var rehabs = record.Topology.Rehabs;
            for (var i = rehabs.Count - 1; i >= 0; i--)
            {
                var node = rehabs[i];
                if (IsNextNode(node)) 
                    return node;
            }

            return null;

            bool IsNextNode(string node)
            {
                return rollingIndex.TryGetValue(node, out var rollingDeployment) && rollingDeployment.State == RollingIndexState.Pending;
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(FinishedNodeTag)] = FinishedNodeTag;
            json[nameof(FinishedAt)] = FinishedAt;
            json[nameof(CompleteAll)] = CompleteAll;
        }
    }
}
