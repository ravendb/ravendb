using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateTopologyCommand : UpdateDatabaseCommand
    {
        public DatabaseTopology Topology;

        public UpdateTopologyCommand() : base(null)
        {
            //
        }

        public UpdateTopologyCommand(string databaseName) : base(databaseName)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Topology = Topology;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Topology)] = Topology.ToJson();
            json[nameof(Etag)] = Etag;
        }
    }
}
