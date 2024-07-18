using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class DeleteIndexCommand : UpdateDatabaseCommand
    {
        public string IndexName { get; set; }

        public DeleteIndexCommand()
        {
            // for deserialization
        }

        public DeleteIndexCommand(string name, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            IndexName = name;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteIndex(IndexName);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
        }
    }
}
