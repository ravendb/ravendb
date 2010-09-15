using System.Collections.Generic;
using System.Linq;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using System;
using Raven.Database.Exceptions;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Implements Unit of Work for accessing a set of sharded RavenDB servers
	/// </summary>
	public class ShardedDocumentSession : IDocumentSession
	{
		/// <summary>
		/// Clears this instance.
		/// Remove all entities from the delete queue and stops tracking changes for all entities.
		/// </summary>
		public void Clear()
		{
			foreach (var shardSession in shardSessions)
			{
				shardSession.Clear();
			}
		}

		/// <summary>
		/// Gets or sets a value indicating whether the session should use optimistic concurrency.
		/// When set to <c>true</c>, a check is made so that a change made behind the session back would fail
		/// and raise <see cref="ConcurrencyException"/>.
		/// </summary>
		/// <value></value>
		public bool UseOptimisticConcurrency
		{
			get
			{
				return shardSessions.All(x => x.UseOptimisticConcurrency);
			}
			set
			{
				foreach (var shardSession in shardSessions)
				{
					shardSession.UseOptimisticConcurrency = value;
				}
			}
		}

		/// <summary>
		/// Gets or sets a value indicating whether non authoritive information is allowed.
		/// Non authoritive information is document that has been modified by a transaction that hasn't been committed.
		/// The server provides the latest committed version, but it is known that attempting to write to a non authoritive document
		/// will fail, because it is already modified.
		/// If set to <c>false</c>, the session will wait <see cref="NonAuthoritiveInformationTimeout"/> for the transaction to commit to get an
		/// authoritive information. If the wait is longer than <see cref="NonAuthoritiveInformationTimeout"/>, <see cref="NonAuthoritiveInformationException"/> is thrown.
		/// </summary>
		/// <value>
		/// 	<c>true</c> if non authoritive information is allowed; otherwise, <c>false</c>.
		/// </value>
		public bool AllowNonAuthoritiveInformation
		{
			get { return shardSessions.First().AllowNonAuthoritiveInformation; }
			set
			{
				foreach (var documentSession in shardSessions)
				{
					documentSession.AllowNonAuthoritiveInformation = value;
				}
			}
		}

		/// <summary>
		/// Gets or sets the timeout to wait for authoritive information if encountered non authoritive document.
		/// </summary>
		/// <value></value>
		public TimeSpan NonAuthoritiveInformationTimeout
		{
			get { return shardSessions.First().NonAuthoritiveInformationTimeout; }
			set
			{
				foreach (var documentSession in shardSessions)
				{
					documentSession.NonAuthoritiveInformationTimeout = value;
				}
			}
		}

		/// <summary>
		/// Gets the number of requests for this session
		/// </summary>
		/// <value></value>
		public int NumberOfRequests
		{
			get { return shardSessions.Sum(x => x.NumberOfRequests); }
		}

		/// <summary>
		/// Occurs when an entity is stored in the session
		/// </summary>
		public event EntityStored Stored;
		/// <summary>
		/// Occurs when an entity is converted to a document and metadata.
		/// Changes made to the document / metadata instances passed to this event will be persisted.
		/// </summary>
	    public event EntityToDocument OnEntityConverted;

		/// <summary>
		/// Gets the metadata for the specified entity.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="instance">The instance.</param>
		/// <returns></returns>
	    public JObject GetMetadataFor<T>(T instance)
	    {
	        var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(instance);
	        return GetSingleShardSession(shardIds).GetMetadataFor(instance);
	    }

		/// <summary>
		/// Gets the document id.
		/// </summary>
		/// <param name="instance">The instance.</param>
		/// <returns></returns>
		public string GetDocumentId(object instance)
		{
			var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(instance);
			return GetSingleShardSession(shardIds).GetDocumentId(instance);
		}

		/// <summary>
		/// Gets a value indicating whether any of the entities tracked by the session has changes.
		/// </summary>
		/// <value></value>
		public bool HasChanges
		{
			get
			{
				return shardSessions.Any(x => x.HasChanges);
			}
		}

		/// <summary>
		/// Determines whether the specified entity has changed.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns>
		/// 	<c>true</c> if the specified entity has changed; otherwise, <c>false</c>.
		/// </returns>
		public bool HasChanged(object entity)
		{
			var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(entity);

			return GetSingleShardSession(shardIds).HasChanged(entity);
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentSession"/> class.
		/// </summary>
		/// <param name="shardStrategy">The shard strategy.</param>
		/// <param name="shardSessions">The shard sessions.</param>
		public ShardedDocumentSession(IShardStrategy shardStrategy, params IDocumentSession[] shardSessions)
		{
			this.shardStrategy = shardStrategy;
			this.shardSessions = shardSessions;

			foreach (var shardSession in shardSessions)
			{
				shardSession.Stored += Stored;
			    shardSession.OnEntityConverted += OnEntityConverted;
			}
		}

		private readonly IShardStrategy shardStrategy;
		private readonly IDocumentSession[] shardSessions;

		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public IDatabaseCommands DatabaseCommands
		{
			get { throw new NotSupportedException("You cannot ask a sharded session for its DatabaseCommands, internal sharded session each have diffeernt DatabaseCommands"); }
		}

		/// <summary>
		/// Loads the specified entity with the specified id.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		public T Load<T>(string id)
		{

			var shardsToUse = GetAppropriateShardedSessions<T>(id);

			//if we can narrow down to single shard, explicitly call it
			if (shardsToUse.Length == 1)
			{
				return shardsToUse[0].Load<T>(id);
			}
			var results = shardStrategy.ShardAccessStrategy.Apply(shardsToUse, x =>
			{
				try
				{
					return new[] { x.Load<T>(id) };
				}
				catch (WebException e)
				{
					var httpWebResponse = e.Response as HttpWebResponse; // we ignore 404, it is expected
					if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
						throw;
					return null;
				}
			});

			return results
				.Where(x => ReferenceEquals(null, x) == false)
				.FirstOrDefault();
		}

		/// <summary>
		/// Loads the specified entities with the specified ids.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		public T[] Load<T>(params string[] ids)
		{
			return shardStrategy.ShardAccessStrategy.Apply(GetAppropriateShardedSessions<T>(null), sessions => sessions.Load<T>(ids)).ToArray();
		}

		/// <summary>
		/// Begin a load while including the specified path
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		public ILoaderWithInclude Include(string path)
		{
			throw new NotSupportedException("Sharded load queries with include aren't supported currently");
		}

		/// <summary>
		/// Gets the document URL for the specified entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public string GetDocumentUrl(object entity)
		{
			if (ReferenceEquals(entity, null))
				throw new ArgumentNullException("entity");

			var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(entity);

			return GetSingleShardSession(shardIds).GetDocumentUrl(entity);
		}

		/// <summary>
		/// Marks the specified entity for deletion. The entity will be deleted when <see cref="IDocumentSession.SaveChanges"/> is called.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		public void Delete<T>(T entity)
		{
			if (ReferenceEquals(entity, null))
				throw new ArgumentNullException("entity");

			var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(entity);

			GetSingleShardSession(shardIds).Delete(entity);
		}

		/// <summary>
		/// Queries the specified index using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <param name="indexName">Name of the index.</param>
		/// <returns></returns>
        public IRavenQueryable<T> Query<T>(string indexName)
	    {
	        throw new NotSupportedException("Sharded linq queries aren't supported currently");
	    }

		/// <summary>
		/// Queries the index specified by <typeparamref name="TIndexCreator"/> using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			throw new NotSupportedException("Sharded linq queries aren't supported currently");
		}

		/// <summary>
		/// Refreshes the specified entity from Raven server.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		public void Refresh<T>(T entity)
        {
            if (ReferenceEquals(entity, null))
                throw new ArgumentNullException("entity");

            var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(entity);

            GetSingleShardSession(shardIds).Refresh(entity);
        }

		private IDocumentSession GetSingleShardSession(string shardId)
		{
			var shardSession = shardSessions.FirstOrDefault(x => x.StoreIdentifier == shardId);
			if (shardSession == null)
				throw new ApplicationException("Can't find a shard with identifier: " + shardId);
			return shardSession;
		}

		/// <summary>
		/// Stores the specified entity in the session. The entity will be saved when <see cref="IDocumentSession.SaveChanges"/> is called.
		/// </summary>
		/// <param name="entity">The entity.</param>
		public void Store(object entity)
		{
			string shardId = shardStrategy.ShardSelectionStrategy.ShardIdForNewObject(entity);
			if (String.IsNullOrEmpty(shardId))
				throw new ApplicationException("Can't find a shard to use for entity: " + entity);

			GetSingleShardSession(shardId).Store(entity);
		}

#if !NET_3_5
		/// <summary>
		/// Stores a dynamic entity
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
        public string StoreDynamic(dynamic entity)
        {
            return Store(entity);
        }
#endif

		/// <summary>
		/// Evicts the specified entity from the session.
		/// Remove the entity from the delete queue and stops tracking changes for this entity.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		public void Evict<T>(T entity)
		{
			string shardId = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(entity);
			if (String.IsNullOrEmpty(shardId))
				throw new ApplicationException("Can't find a shard to use for entity: " + entity);

			GetSingleShardSession(shardId).Evict(entity);
		}

		/// <summary>
		/// Note that while we can assume a transaction for a single shard, cross shard transactions will NOT work.
		/// </summary>
		public void SaveChanges()
		{
			foreach (var shardSession in shardSessions)
			{
				shardSession.SaveChanges();
			}
		}

		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="indexName">Name of the index.</param>
		/// <returns></returns>
		public IDocumentQuery<T> LuceneQuery<T>(string indexName)
		{
			return new ShardedDocumentQuery<T>(indexName,
											   GetAppropriateShardedSessions<T>(null));
		}

		private IDocumentSession[] GetAppropriateShardedSessions<T>(string key)
		{
			var sessionIds =
				shardStrategy.ShardResolutionStrategy.SelectShardIds(ShardResolutionStrategyData.BuildFrom(typeof(T), key));
			IDocumentSession[] documentSessions;
			if (sessionIds != null)
				documentSessions = shardSessions.Where(session => sessionIds.Contains(session.StoreIdentifier)).ToArray();
			else
				documentSessions = shardSessions;
			return documentSessions;
		}

		/// <summary>
		/// Gets the store identifier for this session.
		/// The store identifier is the identifier for the particular RavenDB instance.
		/// This is mostly useful when using sharding.
		/// </summary>
		/// <value>The store identifier.</value>
		public string StoreIdentifier
		{
			get
			{
				return "ShardedSession";
			}
		}

		#region IDisposable Members

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
			foreach (var shardSession in shardSessions)
				shardSession.Dispose();

			//dereference all event listeners
			Stored = null;
		    OnEntityConverted = null;
		}

		#endregion

		/// <summary>
		/// Gets the conventions used by this session
		/// </summary>
		/// <value>The conventions.</value>
		/// <remarks>
		/// This instance is shared among all sessions, changes to the <see cref="DocumentConvention"/> should be done
		/// via the <see cref="IDocumentStore"/> instance, not on a single session.
		/// </remarks>
		public DocumentConvention Conventions
		{
			get { throw new NotSupportedException("You cannot ask a sharded session for its conventions, internal sharded session may each have diffeernt conventions"); }
		}

		/// <summary>
		/// Gets or sets the max number of requests per session.
		/// If the <see cref="NumberOfRequests"/> rise above <see cref="MaxNumberOfRequestsPerSession"/>, an exception will be thrown.
		/// </summary>
		/// <value>The max number of requests per session.</value>
		public int MaxNumberOfRequestsPerSession
	    {
	        get { return shardSessions.First().MaxNumberOfRequestsPerSession; }
	        set
	        {
	            foreach (var documentSession in shardSessions)
	            {
	                documentSession.MaxNumberOfRequestsPerSession = value;
	            }
	        }
	    }        
    }
}