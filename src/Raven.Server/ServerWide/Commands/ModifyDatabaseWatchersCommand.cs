using System.Collections.Generic;
using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ModifyDatabaseWatchersCommand : UpdateDatabaseCommand
    {
        public List<DatabaseWatcher> Watchers;

        public ModifyDatabaseWatchersCommand() : base(null)
        {

        }

        public ModifyDatabaseWatchersCommand(string databaseName) : base(databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Topology.Watchers = new List<DatabaseWatcher>();

            if (Watchers == null)
                return null;

            foreach (var watcher in Watchers)
            {
                watcher.TaskId = (watcher.Database + watcher.Url).ToLower();
                record.Topology.RemoveWatcherIfExists(watcher.TaskId);
                record.Topology.Watchers.Add(watcher);
            }

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            var watchers = new DynamicJsonArray();
            foreach (var w in Watchers)
            {
                watchers.Add(w.ToJson());
            }
            json[nameof(Watchers)] = watchers;
        }
    }

    public class UpdateDatabaseWatcherCommand : UpdateDatabaseCommand
    {
        public DatabaseWatcher Watcher;

        public UpdateDatabaseWatcherCommand() : base(null)
        {

        }

        public UpdateDatabaseWatcherCommand(string databaseName) : base(databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Watcher == null)
                return null;

            var newTaskId = (Watcher.Database + Watcher.Url).ToLower();

            if (Watcher.TaskId != newTaskId)
            {
                //make sure that the new taskId is unique
                record.Topology.RemoveWatcherIfExists(newTaskId);
            }

            if (Watcher.TaskId != null)
            {
                //modified watcher, need to remove the old one
                record.Topology.RemoveWatcherIfExists(Watcher.TaskId);
            }

            Watcher.TaskId = newTaskId;
            record.Topology.Watchers.Add(Watcher);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }
    }
}
