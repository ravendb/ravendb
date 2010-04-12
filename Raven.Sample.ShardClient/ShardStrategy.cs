using Raven.Client.Shard;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Sample.ShardClient
{
    public class ShardStrategy : IShardStrategy
    {
    	public IShardSelectionStrategy ShardSelectionStrategy { get; set; }

    	public IShardResolutionStrategy ShardResolutionStrategy { get; set; }

		public IShardAccessStrategy ShardAccessStrategy { get; set; }

    	public ShardStrategy()
    	{
    		ShardAccessStrategy = new ParallelShardAccessStrategy();
    		ShardSelectionStrategy = new ShardSelectionByRegion();
    		ShardResolutionStrategy = new AllShardsResolutionStrategy();
    	}

    }
}
