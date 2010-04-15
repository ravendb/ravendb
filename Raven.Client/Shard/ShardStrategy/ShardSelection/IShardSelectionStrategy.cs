namespace Raven.Client.Shard.ShardStrategy.ShardSelection
{
    public interface IShardSelectionStrategy
    {
        string SelectShardIdForNewObject(object obj);
        string SelectShardIdForExistingObject(object obj);
    }
}
