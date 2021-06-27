using System.Collections.Generic;
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

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            if (Definition.TaskId == 0)
            {
                Definition.TaskId = index;
            }
            else
            {
                foreach (var task in record.HubPullReplications)
                {
                    if (task.TaskId != Definition.TaskId)
                        continue;
                    record.HubPullReplications.Remove(task);
                    break;
                }
            }
            
            record.EnsureTaskNameIsNotUsed(Definition.Name);
            record.HubPullReplications.Add(Definition);

            record.ClusterState.LastReplicationsIndex = index;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = Definition.ToJson();
        }
    }
}
