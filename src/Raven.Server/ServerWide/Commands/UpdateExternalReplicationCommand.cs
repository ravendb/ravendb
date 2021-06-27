using System;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateExternalReplicationCommand : UpdateDatabaseCommand
    {
        public ExternalReplication Watcher;

        public UpdateExternalReplicationCommand()
        {

        }

        public UpdateExternalReplicationCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            Watcher.AssertValidReplication();

            if (Watcher == null)
                return ;

            if (Watcher.TaskId == 0)
            {
                Watcher.TaskId = index;                
            }
            else
            {
                //modified watcher, remove the old one
                ExternalReplicationBase.RemoveExternalReplication(record.ExternalReplications, Watcher.TaskId);
            }
            //this covers the case of a new watcher and edit of an old watcher
            if (string.IsNullOrEmpty(Watcher.Name))
            {
                Watcher.Name = record.EnsureUniqueTaskName(Watcher.GetDefaultTaskName());
            }

            if (Watcher.Name.StartsWith(ServerWideExternalReplication.NamePrefix, StringComparison.OrdinalIgnoreCase))
            {
                var isNewTask = record.ExternalReplications.Exists(x => x.Name.Equals(Watcher.Name, StringComparison.OrdinalIgnoreCase));
                throw new InvalidOperationException($"Can't {(isNewTask ? "create" : "update")} task: '{Watcher.Name}'. " +
                                                    $"A regular (non server-wide) external replication name can't start with prefix '{ServerWideExternalReplication.NamePrefix}'");
            }

            EnsureTaskNameIsNotUsed(record, Watcher.Name);

            record.ExternalReplications.Add(Watcher);
            record.ClusterState.LastReplicationsIndex = index;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }
    }
}
