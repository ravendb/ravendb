using System;
using System.Threading;
using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Sample.ComplexSharding
{
	public class BlogShardSelectionStrategy : IShardSelectionStrategy
	{
		private readonly int numberOfShardsForPosts;
		private int currentNewShardId;

		public BlogShardSelectionStrategy(int numberOfShardsForPosts)
		{
			this.numberOfShardsForPosts = numberOfShardsForPosts;
		}

		public string ShardIdForNewObject(object obj)
		{
			var shardId = GetShardIdFromObjectType(obj);
			if(obj is Post)
			{
				var nextPostShardId = Interlocked.Increment(ref currentNewShardId) % numberOfShardsForPosts;
				nextPostShardId += 1;// to ensure base 1
				((Post) obj).Id = "posts/" + nextPostShardId + "/"; // encode the shard id in the in the prefix.
				shardId += nextPostShardId;
			}
			return shardId;
		}

		public string ShardIdForExistingObject(object obj)
		{
			var shardId = GetShardIdFromObjectType(obj);
			if(obj is Post)
			{
				// the format of a post id is: 'posts' / 'shard id' / 'post id'
				var id = ((Post)obj).Id.Split(new[]{'/'},StringSplitOptions.RemoveEmptyEntries);
				shardId += id[1];// add shard id
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
	}
}
