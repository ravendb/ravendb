using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Shard.ShardStrategy;

namespace Raven.Client.Shard
{
	public class ShardedDocumentStore : IDocumentStore
	{
        public event Action<string, object> Stored;

        public ShardedDocumentStore(IShardStrategy shardStrategy, Shards shards)
        {
            if (shards == null || shards.Count == 0) 
				throw new ArgumentException("Must have one or more shards", "shards");
            if (shardStrategy == null)
				throw new ArgumentException("Must have shard strategy", "shardStrategy");

            this.shardStrategy = shardStrategy;
            this.shards = shards;
        }

		private readonly IShardStrategy shardStrategy;
		private readonly Shards shards;

        public string Identifier { get; set; }
        
		#region IDisposable Members

		public void Dispose()
		{
            Stored = null;

            foreach (var shard in shards)
                shard.Dispose();
		}

		#endregion

        public IDocumentSession OpenSession()
        {
            return new ShardedDocumentSession(shardStrategy, shards.Select(x => x.OpenSession()).ToArray());
        }

        public IDocumentStore Initialise()
		{
			try
			{
                foreach (var shard in shards)
                {
                    shard.Stored += Stored;
                    shard.Initialise();
                }
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}

            return this;
		}
	}
}