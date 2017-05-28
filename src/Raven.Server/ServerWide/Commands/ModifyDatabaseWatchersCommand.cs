using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Server.Rachis;
using Sparrow.Json;
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

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Topology.Watchers = new Dictionary<string, DatabaseWatcher>();

            if (Watchers != null)
            {
                foreach (var watcher in Watchers)
                {
                    watcher.CurrentTaskId = watcher.GetTaskKey().ToString();
                    record.Topology.Watchers[watcher.CurrentTaskId] = watcher;
                }
            }
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

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Watcher == null)
                return;
            var newTaskKey = Watcher.GetTaskKey().ToString();
            if (Watcher.CurrentTaskId != null && Watcher.CurrentTaskId != newTaskKey)
            {
                //TODO: change to ulong after grish's fix is merged
                record.Topology.Watchers.Remove(Watcher.CurrentTaskId); 
            }

            record.Topology.Watchers[newTaskKey] = Watcher;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watcher)] = Watcher.ToJson();
        }
    }
}
