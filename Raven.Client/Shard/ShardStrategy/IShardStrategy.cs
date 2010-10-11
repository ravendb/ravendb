using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Client.Shard.ShardStrategy
{
	/// <summary>
	/// The shard strategy define how we access, select and resolve specific shards
	/// inside the <see cref="ShardedDocumentStore"/>.
	/// </summary>
    public interface IShardStrategy
    {
		/// <summary>
		/// Gets the shard selection strategy.
		/// </summary>
		/// <value>The shard selection strategy.</value>
        IShardSelectionStrategy ShardSelectionStrategy { get; }
		/// <summary>
		/// Gets the shard resolution strategy.
		/// </summary>
		/// <value>The shard resolution strategy.</value>
        IShardResolutionStrategy ShardResolutionStrategy { get; }
		/// <summary>
		/// Gets the shard access strategy.
		/// </summary>
		/// <value>The shard access strategy.</value>
        IShardAccessStrategy ShardAccessStrategy { get; }
    }
}
