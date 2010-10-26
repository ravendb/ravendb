using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Client.Shard.ShardStrategy
{
	/// <summary>
	/// Default shard strategy for the sharding document store
	/// </summary>
	public class ShardStrategy : IShardStrategy
	{
		/// <summary>
		/// Gets or sets the shard selection strategy.
		/// </summary>
		/// <value>The shard selection strategy.</value>
		public IShardSelectionStrategy ShardSelectionStrategy { get; set; }
		/// <summary>
		/// Gets or sets the shard resolution strategy.
		/// </summary>
		/// <value>The shard resolution strategy.</value>
		public IShardResolutionStrategy ShardResolutionStrategy { get; set; }
		/// <summary>
		/// Gets or sets the shard access strategy.
		/// </summary>
		/// <value>The shard access strategy.</value>
		public IShardAccessStrategy ShardAccessStrategy { get; set; }
	}
}
