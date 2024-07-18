using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes;

public class DeleteIndexHistoryCommand : UpdateDatabaseCommand
{
    public string IndexName { get; set; }

    public DeleteIndexHistoryCommand()
    {
        //deserialization
    }
    
    public DeleteIndexHistoryCommand(string indexName, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
        IndexName = indexName;
    }
    
    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        if (record.IndexesHistory != null)
        {
            record.IndexesHistory.Remove(IndexName);
        }
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(IndexName)] = IndexName;
    }

    public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
    {
    }
}
