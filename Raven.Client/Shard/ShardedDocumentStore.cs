using System;
using System.Collections.Specialized;
using System.Linq;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Shard.ShardStrategy;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Implements a sharded document store
	/// Hiding most sharding details behind this and the <see cref="ShardedDocumentSession"/> gives you the ability to use
	/// sharding without really thinking about this too much
	/// </summary>
	public class ShardedDocumentStore : IDocumentStore
	{
		/// <summary>
		/// Gets the shared operations headers.
		/// </summary>
		/// <value>The shared operations headers.</value>
		public NameValueCollection SharedOperationsHeaders
		{
			get { throw new NotSupportedException("Sharded document store doesn't have a SharedOperationsHeaders. you need to explicitly use the shard instances to get access to the SharedOperationsHeaders"); }
		}

		/// <summary>
		/// Occurs when an entity is stored inside any session opened from this instance
		/// </summary>
		public event EventHandler<StoredEntityEventArgs> Stored;

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentStore"/> class.
		/// </summary>
		/// <param name="shardStrategy">The shard strategy.</param>
		/// <param name="shards">The shards.</param>
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

		public IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
		{
			foreach (var shard in shards)
			{
				shard.RegisterListener(documentStoreListener);
			}
			return this;
		}

		public IDocumentSession OpenSession()
        {
            return new ShardedDocumentSession(shardStrategy, shards.Select(x => x.OpenSession()).ToArray());
        }

	    public IDatabaseCommands DatabaseCommands
	    {
	        get { throw new NotSupportedException("Sharded document store doesn't have a database commands. you need to explicitly use the shard instances to get access to the database commands"); }
	    }

		public DocumentConvention Conventions
		{
			get { throw new NotSupportedException("Sharded document store doesn't have a database conventions. you need to explicitly use the shard instances to get access to the database commands"); }
		}

		public IDocumentStore Initialize()
		{
			try
			{
                foreach (var shard in shards)
                {
                    var currentShard = shard;
                    currentShard.Stored += Stored;
                    var defaultKeyGeneration = currentShard.Conventions.DocumentKeyGenerator == null;
                    currentShard.Initialize();
                    if(defaultKeyGeneration == false)
                        continue;

                    var documentKeyGenerator = currentShard.Conventions.DocumentKeyGenerator;
                    currentShard.Conventions.DocumentKeyGenerator = entity =>
                                                                    currentShard.Identifier + "/" + documentKeyGenerator(entity); 
                }
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}

            return this;
		}

		public IDocumentStore RegisterListener(IDocumentDeleteListener deleteListener)
		{
			foreach (var shard in shards)
			{
				shard.RegisterListener(deleteListener);
			}
			return this;
		}
	}
}