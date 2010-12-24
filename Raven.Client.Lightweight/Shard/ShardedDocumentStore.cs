//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
#if !SILVERLIGHT
using System.Collections.Specialized;
#endif
using System.Linq;
using System.Net;
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
#if !SILVERLIGHT
		public NameValueCollection SharedOperationsHeaders 
#else
		public IDictionary<string,string> SharedOperationsHeaders 
#endif
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

		/// <summary>
		/// Gets or sets the identifier for this store.
		/// </summary>
		/// <value>The identifier.</value>
        public string Identifier { get; set; }
        
		#region IDisposable Members

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
            Stored = null;

            foreach (var shard in shards)
                shard.Dispose();
		}

		#endregion

		/// <summary>
		/// Registers the store listener.
		/// </summary>
		/// <param name="documentStoreListener">The document store listener.</param>
		/// <returns></returns>
		public IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
		{
			foreach (var shard in shards)
			{
				shard.RegisterListener(documentStoreListener);
			}
			return this;
		}

#if !SILVERLIGHT
		/// <summary>
		/// Opens the session.
		/// </summary>
		/// <returns></returns>
		public IDocumentSession OpenSession()
        {
            return new ShardedDocumentSession(shardStrategy, shards.Select(x => x.OpenSession()).ToArray());
        }

        /// <summary>
        /// Opens the session for a particular database
        /// </summary>
	    public IDocumentSession OpenSession(string database)
	    {
	        return new ShardedDocumentSession(shardStrategy, shards.Select(x => x.OpenSession(database)).ToArray());
	    }

        /// <summary>
        /// Opens the session for a particular database with the specified credentials
        /// </summary>
	    public IDocumentSession OpenSession(string database, ICredentials credentialsForSession)
	    {
            return new ShardedDocumentSession(shardStrategy, shards.Select(x => x.OpenSession(database, credentialsForSession)).ToArray());
	    }

        /// <summary>
        /// Opens the session with the specified credentials.
        /// </summary>
        /// <param name="credentialsForSession">The credentials for session.</param>
	    public IDocumentSession OpenSession(ICredentials credentialsForSession)
	    {
            return new ShardedDocumentSession(shardStrategy, shards.Select(x => x.OpenSession(credentialsForSession)).ToArray());
	    }

	    /// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
	    public IDatabaseCommands DatabaseCommands
	    {
	        get { throw new NotSupportedException("Sharded document store doesn't have a database commands. you need to explicitly use the shard instances to get access to the database commands"); }
	    }
#endif

		/// <summary>
		/// Gets the conventions.
		/// </summary>
		/// <value>The conventions.</value>
		public DocumentConvention Conventions
		{
			get { throw new NotSupportedException("Sharded document store doesn't have a database conventions. you need to explicitly use the shard instances to get access to the database commands"); }
		}

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		/// <returns></returns>
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

		/// <summary>
		/// Registers the delete listener.
		/// </summary>
		/// <param name="deleteListener">The delete listener.</param>
		/// <returns></returns>
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
