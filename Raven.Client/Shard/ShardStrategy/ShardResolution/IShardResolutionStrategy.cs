using System.Collections.Generic;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
    public interface IShardResolutionStrategy
    {
        IList<string> SelectShardIdsFromData(ShardResolutionStrategyData srsd);
    }
}
