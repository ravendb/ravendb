using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdatePullReplicationAsSinkCommand : UpdateDatabaseCommand
    {
        public PullReplicationAsSink PullReplicationAsSink;

        public UpdatePullReplicationAsSinkCommand() : base(null)
        {

        }

        public UpdatePullReplicationAsSinkCommand(string databaseName) : base(databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (PullReplicationAsSink == null)
                return null;

            if (PullReplicationAsSink.TaskId == 0)
            {
                PullReplicationAsSink.TaskId = etag;
            }
            else
            {
                ExternalReplication.RemoveExternalReplication(record.SinkPullReplications, PullReplicationAsSink.TaskId);
            }

            if (string.IsNullOrEmpty(PullReplicationAsSink.Name))
            {
                PullReplicationAsSink.Name = record.EnsureUniqueTaskName(PullReplicationAsSink.GetDefaultTaskName());
            }
            record.EnsureTaskNameIsNotUsed(PullReplicationAsSink.Name);
            record.SinkPullReplications.Add(PullReplicationAsSink);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PullReplicationAsSink)] = PullReplicationAsSink.ToJson();
        }
    }
}
