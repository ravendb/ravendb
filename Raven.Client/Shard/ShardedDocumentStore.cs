using System;
using System.Linq;
using Raven.Database;
using Raven.Client.Interface;

namespace Raven.Client.Shard
{
	public class ShardedDocumentStore : IDisposable, IDocumentStore
	{
        public event Action<string, int, object> Stored;

        public ShardedDocumentStore(IShardSelectionStrategy shardSelectionStrategy, params IDocumentStore[] shards)
        {
            if (shards == null || shards.Length == 0) throw new ApplicationException("Must have one or more shards");
            if (shardSelectionStrategy == null) throw new ApplicationException("Must have shard selection strategy");

            this.shardSelectionStrategy = shardSelectionStrategy;
            this.shards = shards;
        }

        IShardSelectionStrategy shardSelectionStrategy;
        IDocumentStore[] shards;

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
            return new ShardedDocumentSession(shardSelectionStrategy, shards.Select(x => x.OpenSession()).ToArray());
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