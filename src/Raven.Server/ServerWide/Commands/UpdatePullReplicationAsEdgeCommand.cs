using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdatePullReplicationAsEdgeCommand : UpdateDatabaseCommand
    {
        public PullReplicationAsEdge PullReplicationAsEdge;

        public UpdatePullReplicationAsEdgeCommand() : base(null)
        {

        }

        public UpdatePullReplicationAsEdgeCommand(string databaseName) : base(databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (PullReplicationAsEdge == null)
                return null;

            if (PullReplicationAsEdge.TaskId == 0)
            {
                PullReplicationAsEdge.TaskId = etag;
            }
            else
            {
                ExternalReplication.RemoveExternalReplication(record.EdgePullReplications, PullReplicationAsEdge.TaskId);
            }

            if (string.IsNullOrEmpty(PullReplicationAsEdge.Name))
            {
                PullReplicationAsEdge.Name = record.EnsureUniqueTaskName(PullReplicationAsEdge.GetDefaultTaskName());
            }
            record.EnsureTaskNameIsNotUsed(PullReplicationAsEdge.Name);
            record.EdgePullReplications.Add(PullReplicationAsEdge);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PullReplicationAsEdge)] = PullReplicationAsEdge.ToJson();
        }
    }
}
