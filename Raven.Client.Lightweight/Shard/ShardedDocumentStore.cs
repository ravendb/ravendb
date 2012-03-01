#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
#if !SILVERLIGHT
using System.Collections.Generic;
using System.Collections.Specialized;
#endif
using System.Linq;
using System.Net;
using Raven.Abstractions.Extensions;
#if !NET_3_5
using Raven.Client.Connection.Async;
#endif
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Shard.ShardStrategy;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Implements a sharded document store
	/// Hiding most sharding details behind this and the <see cref="ShardedDocumentSession"/> gives you the ability to use
	/// sharding without really thinking about this too much
	/// </summary>
	public class ShardedDocumentStore : DocumentStoreBase
	{
		private readonly IShardStrategy shardStrategy;
		private readonly List<IDocumentStore> shards;

#if !SILVERLIGHT
		/// <summary>
		/// Gets the shared operations headers.
		/// </summary>
		/// <value>The shared operations headers.</value>
		/// <exception cref="NotSupportedException"></exception>
		public override NameValueCollection SharedOperationsHeaders 
#else
		public IDictionary<string,string> SharedOperationsHeaders 
#endif
		{
			get { throw new NotSupportedException("Sharded document store doesn't have a SharedOperationsHeaders. you need to explicitly use the shard instances to get access to the SharedOperationsHeaders"); }
			protected set { throw new NotSupportedException("Sharded document store doesn't have a SharedOperationsHeaders. you need to explicitly use the shard instances to get access to the SharedOperationsHeaders"); }
		}

		/// <summary>
		/// Get the <see cref="HttpJsonRequestFactory"/> for this store
		/// </summary>
		public override HttpJsonRequestFactory JsonRequestFactory
		{
			get { throw new NotSupportedException("Sharded document store doesn't have a JsonRequestFactory. you need to explicitly use the shard instances to get access to the JsonRequestFactory"); }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentStore"/> class.
		/// </summary>
		/// <param name="shardStrategy">The shard strategy.</param>
		/// <param name="shards">The shards.</param>
		public ShardedDocumentStore(IShardStrategy shardStrategy, List<IDocumentStore> shards)
		{
			if (shards == null || shards.Count == 0) 
				throw new ArgumentException("Must have one or more shards", "shards");
			if (shardStrategy == null)
				throw new ArgumentException("Must have shard strategy", "shardStrategy");

			this.shardStrategy = shardStrategy;
			this.shards = shards;
		}

		/// <summary>
		/// Gets or sets the identifier for this store.
		/// </summary>
		/// <value>The identifier.</value>
		public override string Identifier { get; set; }
		
		#region IDisposable Members

		/// <summary>
		/// Called after dispose is completed
		/// </summary>
		public override event EventHandler AfterDispose;

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public override void Dispose()
		{
			foreach (var shard in shards)
				shard.Dispose();

			WasDisposed = true;

			var afterDispose = AfterDispose;
			if (afterDispose != null)
				afterDispose(this, EventArgs.Empty);

		}

		#endregion


#if !NET_3_5
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		public override IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { throw new NotSupportedException("Sharded document store doesn't have a database commands. you need to explicitly use the shard instances to get access to the database commands"); }
		}
		
		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		public override IAsyncDocumentSession OpenAsyncSession()
		{
			throw new NotSupportedException("Shared document store doesn't support async operations");
		}

		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		public override IAsyncDocumentSession OpenAsyncSession(string databaseName)
		{
			throw new NotSupportedException("Shared document store doesn't support async operations");
		}

#endif

		/// <summary>
		/// Setup the context for aggressive caching.
		/// </summary>
		/// <param name="cacheDuration">Specify the aggressive cache duration</param>
		/// <remarks>
		/// aggressive caching means that we will not check the server to see whatever the response
		/// we provide is current or not, but will serve the information directly from the local cache
		/// without touching the server.
		/// </remarks>
		public override IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
		{
			var disposables = shards.Select(shard => shard.AggressivelyCacheFor(cacheDuration)).ToList();

			return new DisposableAction(() =>
			{
				foreach (var disposable in disposables)
				{
					disposable.Dispose();
				}
			});
		}

		/// <summary>
		/// Setup the context for no aggressive caching
		/// </summary>
		/// <remarks>
		/// This is mainly useful for internal use inside RavenDB, when we are executing
		/// queries that has been marked with WaitForNonStaleResults, we temporarily disable
		/// aggressive caching.
		/// </remarks>
		public override IDisposable DisableAggressiveCaching()
		{
			var disposables = shards.Select(shard => shard.DisableAggressiveCaching()).ToList();

			return new DisposableAction(() =>
			{
				foreach (var disposable in disposables)
				{
					disposable.Dispose();
				}
			});
		}

		/// <summary>
		/// The current session id - only used during contsruction
		/// </summary>
		[ThreadStatic]
		protected static Guid? currentSessionId;

//#if !NET_3_5
//        private Func<IAsyncDatabaseCommands> asyncShardedDbCommandsGenerator;
//        /// <summary>
//        /// Gets the async database commands.
//        /// </summary>
//        /// <value>The async database commands.</value>
//        public override IAsyncDatabaseCommands AsyncDatabaseCommands
//        {
//            get
//            {
//                if (asyncShardedDbCommandsGenerator == null)
//                    return null;
//                return asyncShardedDbCommandsGenerator();
//            }
//        }
//#endif

#if !SILVERLIGHT
		
		/// <summary>
		/// Opens the session.
		/// </summary>
		/// <returns></returns>
		public override IDocumentSession OpenSession()
		{
			EnsureNotClosed();

			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new ShardedDocumentSession(this, listeners, sessionId, shardStrategy, shards.ToDictionary(x => x.Identifier, x => x.DatabaseCommands)
//#if !NET_3_5
//, AsyncDatabaseCommands
//#endif
);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		/// <summary>
		/// Opens the session for a particular database
		/// </summary>
		public override IDocumentSession OpenSession(string database)
		{
			EnsureNotClosed();

			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new ShardedDocumentSession(this, listeners, sessionId, shardStrategy,
					shards.ToDictionary(x => x.Identifier, x => x.DatabaseCommands.ForDatabase(database))
//#if !NET_3_5
//                    , AsyncDatabaseCommands.ForDatabase(database)
//#endif
);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		/// <summary>
		/// Opens the session for a particular database with the specified credentials
		/// </summary>
		public override IDocumentSession OpenSession(string database, ICredentials credentialsForSession)
		{
			EnsureNotClosed();

			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new ShardedDocumentSession(this, listeners, sessionId, shardStrategy,
					shards.ToDictionary(x => x.Identifier, x => x.DatabaseCommands.ForDatabase(database).With(credentialsForSession))
//#if !NET_3_5
//                    , AsyncDatabaseCommands.ForDatabase(database).With(credentialsForSession)
//#endif
);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		/// <summary>
		/// Opens the session with the specified credentials.
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		public override IDocumentSession OpenSession(ICredentials credentialsForSession)
		{
			EnsureNotClosed();

			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new ShardedDocumentSession(this, listeners, sessionId, shardStrategy,
					shards.ToDictionary(x => x.Identifier, x => x.DatabaseCommands.With(credentialsForSession))
//#if !NET_3_5
//                    , AsyncDatabaseCommands.With(credentialsForSession)
//#endif
);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public override IDatabaseCommands DatabaseCommands
		{
			get { throw new NotSupportedException("Sharded document store doesn't have a database commands. you need to explicitly use the shard instances to get access to the database commands"); }
		}
#endif

		/// <summary>
		/// Gets or sets the URL.
		/// </summary>
		public override string Url
		{
			get { throw new NotImplementedException("There isn't a singular url when using sharding"); }
		}

		///<summary>
		/// Gets the etag of the last document written by any session belonging to this 
		/// document store
		///</summary>
		public override Guid? GetLastWrittenEtag()
		{
			throw new NotImplementedException("This isn't a single last written etag when sharding");
		}

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		/// <returns></returns>
		public override IDocumentStore Initialize()
		{
			try
			{
				shards.ForEach(shard => shard.Initialize());
				Conventions = shards.First().Conventions;
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
#endif
