using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Shard
{
    public class AllShardsResolutionStrategy : IShardResolutionStrategy
    {
        public IList<string> SelectShardIdsFromData(ShardResolutionStrategyData srsd)
        {
            //will force it to use all shards
            return null;
        }
    }
}
