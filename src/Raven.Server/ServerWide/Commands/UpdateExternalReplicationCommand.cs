using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateExternalReplicationCommand : UpdateDatabaseCommand
    {
        public DatabaseWatcher Watcher;

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

            record.Topology.EnsureUniqueDbAndUrl(Watcher);

            if (Watcher.TaskId == 0)
            {
                Watcher.TaskId = etag;
            }
            else
            {
                //modified watcher, remove the old one
                record.Topology.RemoveWatcher(Watcher.TaskId);
            }

            record.Topology.Watchers.Add(Watcher);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }
    }
}
