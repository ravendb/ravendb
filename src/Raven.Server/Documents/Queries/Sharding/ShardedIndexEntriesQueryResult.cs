namespace Raven.Server.Documents.Queries.Sharding;

public class ShardedIndexEntriesQueryResult : IndexEntriesQueryResult
{

    public ShardedIndexEntriesQueryResult() : base(indexDefinitionRaftIndex: null)
    {
    }
}
