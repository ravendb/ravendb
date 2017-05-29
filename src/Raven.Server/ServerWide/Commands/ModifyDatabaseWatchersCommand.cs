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
            if (Watchers != null)
            {
                record.Topology.Watchers = Watchers;
            }
            else
            {
                record.Topology.Watchers = new List<DatabaseWatcher>();
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
}
