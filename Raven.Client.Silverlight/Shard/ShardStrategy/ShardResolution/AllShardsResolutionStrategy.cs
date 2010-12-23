//-----------------------------------------------------------------------
// <copyright file="AllShardsResolutionStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
	/// <summary>
	/// Shard resolution strategy that select all the shards
	/// </summary>
    public class AllShardsResolutionStrategy : IShardResolutionStrategy
    {
		/// <summary>
		/// Selects the shard ids appropriate for the given data
		/// </summary>
        public IList<string> SelectShardIds(ShardResolutionStrategyData srsd)
        {
            //will force it to use all shards
            return null;
        }
    }
}
