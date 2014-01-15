using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.RavenFS.Connections;

namespace Raven.Client.RavenFS.Shard
{
	/// <summary>
	/// Default shard strategy for the sharding document store
	/// </summary>
	public class ShardStrategy
	{
		private readonly IDictionary<string, RavenFileSystemClient> shards;

		public delegate string ModifyFileNameFunc(FileConvention convention, string shardId, string filename);

		public ShardStrategy(IDictionary<string, RavenFileSystemClient> shards)
		{
			if (shards == null) throw new ArgumentNullException("shards");
			if (shards.Count == 0)
				throw new ArgumentException("Shards collection must have at least one item", "shards");

			this.shards = new Dictionary<string, RavenFileSystemClient>(shards, StringComparer.OrdinalIgnoreCase);


			Conventions = shards.First().Value.Convention.Clone();

			ShardAccessStrategy = new SequentialShardAccessStrategy();
			ShardResolutionStrategy = new DefaultShardResolutionStrategy(shards.Keys, this);
			ModifyFileName = (convention, shardId, documentId) => convention.IdentityPartsSeparator + shardId + convention.IdentityPartsSeparator + documentId;
		}

		public FileConvention Conventions { get; set; }

		/// <summary>
		/// Gets or sets the shard resolution strategy.
		/// </summary>
		public IShardResolutionStrategy ShardResolutionStrategy { get; set; }

		/// <summary>
		/// Gets or sets the shard access strategy.
		/// </summary>
		public IShardAccessStrategy ShardAccessStrategy { get; set; }

		/// <summary>
		/// Get or sets the modification for the document id for sharding
		/// </summary>
		public ModifyFileNameFunc ModifyFileName { get; set; }

		public IDictionary<string, RavenFileSystemClient> Shards
		{
			get { return shards; }
		}

		public int StableHashString(string text)
		{
			unchecked
			{
				return text.ToCharArray().Aggregate(11, (current, c) => current * 397 + c);
			}
		}
	}
}
