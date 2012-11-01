using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Shard;

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

		public string GenerateShardIdFor(object entity, ITransactionalDocumentSession sessionMetadata)
		{
			return GetShardIdFromObjectType(entity);
		}

		public string MetadataShardIdFor(object entity)
		{
			return GetShardIdFromObjectType(entity, true);
		}

		private string GetShardIdFromObjectType(object instance, bool requiredMaster = false)
		{
			if (instance is User)
				return "Users";
			if (instance is Blog)
				return "Blogs";
			if (instance is Post)
			{
				if (requiredMaster)
					return "Posts01";

				var nextPostShardId = Interlocked.Increment(ref currentNewShardId) % numberOfShardsForPosts + 1;
				return "Posts" + nextPostShardId.ToString("D2");
			}
			throw new ArgumentException("Cannot get shard id for '" + instance + "' because it is not a User, Blog or Post");
		}

		public IList<string> PotentialShardsFor(ShardRequestData requestData)
		{
			if (requestData.EntityType == typeof(User))
				return new[] { "Users" };
			if (requestData.EntityType == typeof(Blog))
				return new[] { "Blogs" };
			if (requestData.EntityType == typeof (Post) 
				|| requestData.EntityType == typeof (TotalVotesUp.ReduceResult)
				|| requestData.EntityType == typeof (TotalPostsPerDay.ReduceResult)
				)
				return Enumerable.Range(0, numberOfShardsForPosts).Select(i => "Posts" + (i + 1).ToString("D2")).ToArray();

			throw new ArgumentException("Cannot get shard id for '" + requestData.EntityType + "' because it is not a User, Blog or Post");
		}
	}
}