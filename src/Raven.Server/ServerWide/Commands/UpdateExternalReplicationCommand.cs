using System.Linq;
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

            ExternalReplication.EnsureUniqueDbAndUrl(record.ExternalReplication,Watcher);

            if (Watcher.TaskId == 0)
            {
                Watcher.TaskId = etag;
            }
            else
            {
                //modified watcher, remove the old one
                ExternalReplication.RemoveWatcher(ref record.ExternalReplication, Watcher.TaskId);
            }

            record.ExternalReplication.Add(Watcher);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }
    }

    // we discover the external replication destination url only after we try to communicate with it's cluster.
    // After we have it we need to update the actual destination silently without triggering any events.
    public class UpdateExternalReplicationCommandSilently : UpdateDatabaseCommand
    {
        public long TaskId;
        public string Destination;

        public UpdateExternalReplicationCommandSilently() : base(null)
        {

        }

        public UpdateExternalReplicationCommandSilently(string databaseName, long taskId, string destination) : base(databaseName)
        {
            TaskId = taskId;
            Destination = destination;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.ExternalReplication.Single(e => e.TaskId == TaskId).Url = Destination;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(Destination)] = Destination;
        }
    }
}
