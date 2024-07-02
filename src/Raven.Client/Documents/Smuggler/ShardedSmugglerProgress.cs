using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler;

public sealed class ShardedSmugglerProgress : SmugglerResult.SmugglerProgress, IShardedOperationProgress
{
    public int ShardNumber { get; set; }
    public string NodeTag { get; set; }

    public void Fill(IOperationProgress progress, int shardNumber, string nodeTag)
    {
        ShardNumber = shardNumber;
        NodeTag = nodeTag;

        if (progress is not SmugglerResult.SmugglerProgress sp)
            return;

        _result = sp._result;
        Message = sp.Message;
        DatabaseRecord = sp.DatabaseRecord;
        Documents = sp.Documents;
        RevisionDocuments = sp.RevisionDocuments;
        Tombstones = sp.Tombstones;
        Conflicts = sp.Conflicts;
        Identities = sp.Identities;
        Indexes = sp.Indexes;
        CompareExchange = sp.CompareExchange;
        Subscriptions = sp.Subscriptions;
        ReplicationHubCertificates = sp.ReplicationHubCertificates;
        Counters = sp.Counters;
        TimeSeries = sp.TimeSeries;
        CompareExchangeTombstones = sp.CompareExchangeTombstones;
        TimeSeriesDeletedRanges = sp.TimeSeriesDeletedRanges;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(ShardNumber)] = ShardNumber;
        json[nameof(NodeTag)] = NodeTag;
        return json;
    }
}
