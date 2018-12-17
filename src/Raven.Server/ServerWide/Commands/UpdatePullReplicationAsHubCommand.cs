using System;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdatePullReplicationAsHubCommand : UpdateDatabaseCommand
    {
        public PullReplicationDefinition Definition;

        public UpdatePullReplicationAsHubCommand():base(null) { }

        public UpdatePullReplicationAsHubCommand(string databaseName) : base(databaseName)
        {
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Definition.TaskId = etag;
            record.EnsureTaskNameIsNotUsed(Definition.Name);
            record.HubPullReplications[Definition.Name] = Definition;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = Definition.ToJson();
        }
    }
}
