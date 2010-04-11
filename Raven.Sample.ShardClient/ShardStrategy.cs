using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Shard;
using Raven.Client;
using Raven.Client.Shard.ShardStrategy.ShardAccess;

namespace Raven.Sample.ShardClient
{
    public class ShardStrategy : IShardStrategy
    {
        private IShardSelectionStrategy _ShardSelectionStrategy = new ShardSelectionByRegion();
        public IShardSelectionStrategy ShardSelectionStrategy
        {
            get
            {
                return _ShardSelectionStrategy;
            }
        }

        //since we are using all shards for companies, always search all shards
        private IShardResolutionStrategy _ShardResolutionStrategy = new AllShardsResolutionStrategy();
        public IShardResolutionStrategy ShardResolutionStrategy
        {
            get
            {
                return _ShardResolutionStrategy;
            }
        }

        private IShardAccessStrategy _ShardAccessStrategy = new ParallelShardAccessStrategy();
        public IShardAccessStrategy ShardAccessStrategy
        {
            get
            {
                return _ShardAccessStrategy;
            }
        }
    }
}
