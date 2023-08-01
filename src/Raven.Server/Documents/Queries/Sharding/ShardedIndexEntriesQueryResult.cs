namespace Raven.Server.Documents.Queries.Sharding;

public sealed class ShardedIndexEntriesQueryResult : IndexEntriesQueryResult
{

    public ShardedIndexEntriesQueryResult() : base(indexDefinitionRaftIndex: null)
    {
    }
}
