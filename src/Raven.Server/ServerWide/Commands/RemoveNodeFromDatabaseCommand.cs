using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class RemoveNodeFromDatabaseCommand : UpdateDatabaseCommand
    {
        public string NodeTag;
        public string DatabaseId;

        public RemoveNodeFromDatabaseCommand()
        {
        }

        public RemoveNodeFromDatabaseCommand(string databaseName, string databaseId, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            DatabaseId = databaseId;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            DeletionInProgressStatus deletionStatus = DeletionInProgressStatus.No;
            record.DeletionInProgress?.TryGetValue(NodeTag, out deletionStatus);

            record.Topology.RemoveFromTopology(NodeTag);
            record.DeletionInProgress?.Remove(NodeTag);

            if (DatabaseId == null)
                return;

            if (deletionStatus == DeletionInProgressStatus.HardDelete)
            {
                if (record.UnusedDatabaseIds == null)
                    record.UnusedDatabaseIds = new HashSet<string>();

                record.UnusedDatabaseIds.Add(DatabaseId);
            }

            if (record.RollingIndexes == null)
                return;

            foreach (var rollingIndex in record.RollingIndexes)
            {
                if (rollingIndex.Value.ActiveDeployments.TryGetValue(NodeTag, out var deployment))
                {
                    var dummy = new PutRollingIndexCommand(DatabaseName, rollingIndex.Key, NodeTag, DateTime.Now, "dummy update");
                    dummy.UpdateDatabaseRecord(record, etag);
                    rollingIndex.Value.ActiveDeployments.Remove(NodeTag);
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
            json[nameof(DatabaseId)] = DatabaseId;
        }
    }
}
