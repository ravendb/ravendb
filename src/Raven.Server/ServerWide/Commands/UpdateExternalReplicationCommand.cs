using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateExternalReplicationCommand : UpdateDatabaseCommand
    {
        public ExternalReplication Watcher;

        public UpdateExternalReplicationCommand() : base(null)
        {

        }

        public UpdateExternalReplicationCommand(string databaseName) : base(databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Watcher == null)
                return null;

            if (Watcher.TaskId == 0)
            {
                Watcher.TaskId = etag;                
            }
            else
            {
                //modified watcher, remove the old one
                ExternalReplication.RemoveExternalReplication(record.ExternalReplications, Watcher.TaskId);
            }
            //this covers the case of a new watcher and edit of an old watcher
            if (string.IsNullOrEmpty(Watcher.Name))
            {
                Watcher.Name = record.EnsureUniqueTaskName(Watcher.GetDefaultTaskName());
            }
            record.EnsureTaskNameIsNotUsed(Watcher.Name);
            record.ExternalReplications.Add(Watcher);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }
    }
}
