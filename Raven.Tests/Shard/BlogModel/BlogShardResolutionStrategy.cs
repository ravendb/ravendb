using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Shard.ShardResolution;

namespace Raven.Tests.Shard.BlogModel
{
	public class BlogShardResolutionStrategy : IShardResolutionStrategy
	{
		private readonly int numberOfShardsForPosts;
		private int currentNewShardId;

		public BlogShardResolutionStrategy(int numberOfShardsForPosts)
		{
			this.numberOfShardsForPosts = numberOfShardsForPosts;
		}

		public string GenerateShardIdFor(object entity)
		{
			var shardId = GetShardIdFromObjectType(entity);
			var post = entity as Post;
			if (post != null)
			{
				var nextPostShardId = Interlocked.Increment(ref currentNewShardId) % numberOfShardsForPosts + 1;
				shardId += nextPostShardId;
			}
			return shardId;
		}

		private static string GetShardIdFromObjectType(object instance)
		{
			if (instance is User)
				return "Users";
			if (instance is Blog)
				return "Blogs";
			if (instance is Post)
				return "Posts #";
			throw new ArgumentException("Cannot get shard id for '" + instance + "' because it is not a User, Blog or Post");
		}

		public IList<string> PotentialShardsFor(ShardRequestData requestData)
		{
			if (requestData.EntityType == typeof(User))
				return new[] { "Users" };
			if (requestData.EntityType == typeof(Blog))
				return new[] { "Blogs" };
			if (requestData.EntityType == typeof(Post))
			{
				if (requestData.Key == null) // general query
					return Enumerable.Range(0, numberOfShardsForPosts).Select(i => "Posts #" + (i + 1)).ToArray();
				// we can optimize better, since the key has the shard id
				// key structure is 'posts' / 'shard id' / 'post id'
				var parts = requestData.Key.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
				return new[] { "Posts #" + parts[1] };
			}

			throw new ArgumentException("Cannot get shard id for '" + requestData.EntityType + "' because it is not a User, Blog or Post");
		}

		public string MetadataShardIdFor(object entity)
		{
			var shardIdFromObjectType = GetShardIdFromObjectType(entity);
			if (entity is Post)
				return shardIdFromObjectType + "1";
			return shardIdFromObjectType;
		}
	}
}