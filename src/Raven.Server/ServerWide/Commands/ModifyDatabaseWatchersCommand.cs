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
        public BlittableJsonReaderArray Watchers;

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
                record.Topology.Watchers = new List<DatabaseWatcher>(
                    Watchers.Items.Select(
                        i => JsonDeserializationRachis<DatabaseWatcher>.Deserialize((BlittableJsonReaderObject)i)
                    ));
            }
            else
            {
                record.Topology.Watchers = new List<DatabaseWatcher>();
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Watchers)] = Watchers;
        }
    }
}
