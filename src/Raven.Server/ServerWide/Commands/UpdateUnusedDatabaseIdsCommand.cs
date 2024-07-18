using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateUnusedDatabaseIdsCommand : UpdateDatabaseCommand
    {
        public HashSet<string> UnusedDatabaseIds;

        public UpdateUnusedDatabaseIdsCommand()
        {
            
        }
        public UpdateUnusedDatabaseIdsCommand(string database, HashSet<string> list, string raftId) : base(database, raftId)
        {
            UnusedDatabaseIds = list;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.UnusedDatabaseIds = UnusedDatabaseIds;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(UnusedDatabaseIds)] = UnusedDatabaseIds;
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
        }
    }
}
