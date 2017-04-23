using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ModifyDatabaseWatchers : UpdateDatabaseCommand
    {
        public BlittableJsonReaderObject Value;

        public ModifyDatabaseWatchers() : base(null)
        {

        }

        public ModifyDatabaseWatchers(string databaseName) : base(databaseName)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Value.TryGet("NewWatchers", out BlittableJsonReaderArray watchers);

            if (watchers != null)
            {
                record.Topology.Watchers = new List<DatabaseWatcher>(
                    watchers.Items.Select(
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
            json[nameof(Value)] = Value;
        }
    }
}
