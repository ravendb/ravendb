using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes;

public class PutIndexHistoryCommand : UpdateDatabaseCommand
{
    public string IndexName { get; set; }
    public List<IndexHistoryEntry> IndexHistory { get; set; }
    public int RevisionsToKeep { get; set; }

    public PutIndexHistoryCommand()
    {
        //deserialization
    }
    
    public PutIndexHistoryCommand(string indexName, List<IndexHistoryEntry> indexHistory, int revisionsToKeep, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
        IndexName = indexName;
        IndexHistory = indexHistory;
        RevisionsToKeep = revisionsToKeep;
    }
    
    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        record.IndexesHistory ??= new();
        
        if (record.IndexesHistory.TryGetValue(IndexName, out var indexHistoryEntries) == false)
            record.IndexesHistory[IndexName] = IndexHistory;
        else
        {
            foreach (var entry in IndexHistory)
            {
                record.AddIndexHistory(entry.Definition, entry.Source, RevisionsToKeep, entry.CreatedAt, rollingIndexDeployment: entry.RollingDeployment, isFromCommand: true);
            }
        }
    }

    public override void FillJson(DynamicJsonValue json)
    {
        var histJson = new DynamicJsonArray();
        foreach (var ih in IndexHistory)
            histJson.Add(ih.ToJson());
        
        json[nameof(IndexName)] = IndexName;
        json[nameof(IndexHistory)] = histJson;
        json[nameof(RevisionsToKeep)] = RevisionsToKeep;
    }

    public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
    {
    }
}
