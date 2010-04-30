using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Client.Shard.ShardStrategy
{
	public class ShardStrategy : IShardStrategy
	{
		public IShardSelectionStrategy ShardSelectionStrategy { get; set; }
		public IShardResolutionStrategy ShardResolutionStrategy { get; set; }
		public IShardAccessStrategy ShardAccessStrategy { get; set; }
	}
}