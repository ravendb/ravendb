using System;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdatePullReplicationCommand : UpdateDatabaseCommand
    {
        public PullReplicationDefinition PullReplicationDefinition;

        public UpdatePullReplicationCommand():base(null) { }

        public UpdatePullReplicationCommand(string databaseName) : base(databaseName)
        {
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            PullReplicationDefinition.TaskId = etag;
            record.PullReplicationDefinition[PullReplicationDefinition.Name] = PullReplicationDefinition;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PullReplicationDefinition)] = PullReplicationDefinition.ToJson();
        }
    }
}
