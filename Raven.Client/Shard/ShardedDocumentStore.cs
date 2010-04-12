using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Shard.ShardStrategy;
using Raven.Database;
using Raven.Client.Shard;

namespace Raven.Client.Shard
{
	public class ShardedDocumentStore : IDisposable, IDocumentStore
	{
        public event Action<string, int, object> Stored;

        public ShardedDocumentStore(IShardStrategy shardStrategy, Shards shards)
        {
            if (shards == null || shards.Count == 0) throw new ApplicationException("Must have one or more shards");
            if (shardStrategy == null) throw new ApplicationException("Must have shard strategy");

            this.shardStrategy = shardStrategy;
            this.shards = shards;
        }

        IShardStrategy shardStrategy;
        Shards shards;

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
                    shard.Stored += this.Stored;
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