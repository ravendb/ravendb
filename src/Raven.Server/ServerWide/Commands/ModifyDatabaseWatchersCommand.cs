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
            if (Watchers != null)
            {
                record.Topology.Watchers = Watchers;
            }
            else
            {
                record.Topology.Watchers = new List<DatabaseWatcher>();
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
}
