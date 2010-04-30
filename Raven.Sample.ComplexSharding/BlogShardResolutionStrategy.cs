using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Shard.ShardStrategy.ShardResolution;

namespace Raven.Sample.ComplexSharding
{
	public class BlogShardResolutionStrategy : IShardResolutionStrategy
	{
		private readonly int numberOfShardsForPosts;
		public BlogShardResolutionStrategy(int numberOfShardsForPosts)
		{
			this.numberOfShardsForPosts = numberOfShardsForPosts;
		}public IList<string> SelectShardIdsFromData(ShardResolutionStrategyData srsd)
		{
			if (srsd.EntityType == typeof(User))
				return new[] { "Users" };
			if (srsd.EntityType == typeof(Blog))
				return new[] { "Blogs" };
			if (srsd.EntityType == typeof(Post))
				return Enumerable.Range(0, numberOfShardsForPosts).Select(i => "Posts #" + (i + 1)).ToArray();

			throw new ArgumentException("Cannot get shard id for '" + srsd.EntityType + "' because it is not a User, Blog or Post");
		}
	}
}