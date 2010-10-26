using System.Collections.Generic;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
	/// <summary>
	/// Shard resultion strategy that select all the shards
	/// </summary>
    public class AllShardsResolutionStrategy : IShardResolutionStrategy
    {
		/// <summary>
		/// Selects the shard ids appropraite for the given data
		/// </summary>
        public IList<string> SelectShardIds(ShardResolutionStrategyData srsd)
        {
            //will force it to use all shards
            return null;
        }
    }
}
