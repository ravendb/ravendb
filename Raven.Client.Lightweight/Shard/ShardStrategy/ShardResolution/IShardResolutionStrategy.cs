using System.Collections.Generic;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
	/// <summary>
	/// Implementors of this interface provide a way to decide which shards will be queried
	/// for a specified operation
	/// </summary>
    public interface IShardResolutionStrategy
    {
		/// <summary>
		/// Selects the shard ids appropriate for the specified data
		/// </summary>
        IList<string> SelectShardIds(ShardResolutionStrategyData srsd);
    }
}
