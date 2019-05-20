using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdatePullReplicationAsHubCommand : UpdateDatabaseCommand
    {
        public PullReplicationDefinition Definition;

        public UpdatePullReplicationAsHubCommand() { }

        public UpdatePullReplicationAsHubCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Definition.TaskId == 0)
            {
                Definition.TaskId = etag;
            }
            else
            {
                PullReplicationDefinition.RemoveHub(record.HubPullReplications, Definition.TaskId);
            }
            
            record.EnsureTaskNameIsNotUsed(Definition.Name);
            record.HubPullReplications.Add(Definition);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = Definition.ToJson();
        }
    }
}
