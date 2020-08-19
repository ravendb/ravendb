using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
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

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Watcher.AssertValidReplication();

            if (Watcher == null)
                return ;

            if (Watcher.TaskId == 0)
            {
                Watcher.TaskId = etag;                
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

            EnsureTaskNameIsNotUsed(record, Watcher.Name);
            record.ExternalReplications.Add(Watcher);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }
    }
}
