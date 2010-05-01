namespace Raven.Client.Shard.ShardStrategy.ShardSelection
{
    public interface IShardSelectionStrategy
    {
        string ShardIdForNewObject(object obj);
        string ShardIdForExistingObject(object obj);
    }
}
