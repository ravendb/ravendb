using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateUnusedDatabaseIdsCommand : UpdateDatabaseCommand
    {
        public List<string> UnusedDatabaseIds;

        public UpdateUnusedDatabaseIdsCommand()
        {
            
        }
        public UpdateUnusedDatabaseIdsCommand(string database, List<string> list, string raftId) : base(database, raftId)
        {
            UnusedDatabaseIds = list;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.UnusedDatabaseIds = UnusedDatabaseIds;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(UnusedDatabaseIds)] = UnusedDatabaseIds;
        }
    }
}
