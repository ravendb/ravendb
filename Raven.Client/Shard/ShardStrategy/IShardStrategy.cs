using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Client.Shard.ShardStrategy
{
    public interface IShardStrategy
    {
        IShardSelectionStrategy ShardSelectionStrategy { get; }
        IShardResolutionStrategy ShardResolutionStrategy { get; }
        IShardAccessStrategy ShardAccessStrategy { get; }
    }
}
