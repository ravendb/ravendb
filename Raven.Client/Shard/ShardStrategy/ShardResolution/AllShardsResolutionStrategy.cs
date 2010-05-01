using System.Collections.Generic;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
    public class AllShardsResolutionStrategy : IShardResolutionStrategy
    {
        public IList<string> SelectShardIds(ShardResolutionStrategyData srsd)
        {
            //will force it to use all shards
            return null;
        }
    }
}
