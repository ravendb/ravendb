using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Shard
{
    public interface IShardResolutionStrategy
    {
        IList<string> SelectShardIdsFromData(ShardResolutionStrategyData srsd);
    }
}
