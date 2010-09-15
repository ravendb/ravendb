using System.Collections.Generic;
using System.Linq;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Client.Document;
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

	    public JObject GetMetadataFor<T>(T instance)
	    {
	        var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(instance);
	        return GetSingleShardSession(shardIds).GetMetadataFor(instance);
	    }

		public string GetDocumentId(object instance)
		{
			var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(instance);
			return GetSingleShardSession(shardIds).GetDocumentId(instance);
		}

		public bool HasChanges
		{
			get
			{
				return shardSessions.Any(x => x.HasChanges);
			}
		}

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

		public IDatabaseCommands DatabaseCommands
		{
			get { throw new NotSupportedException("You cannot ask a sharded session for its DatabaseCommands, internal sharded session each have diffeernt DatabaseCommands"); }
		}

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

		public string GetDocumentUrl(object entity)
		{
			if (ReferenceEquals(entity, null))
				throw new ArgumentNullException("entity");

			var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(entity);

			return GetSingleShardSession(shardIds).GetDocumentUrl(entity);
		}

		public void Delete<T>(T entity)
		{
			if (ReferenceEquals(entity, null))
				throw new ArgumentNullException("entity");

			var shardIds = shardStrategy.ShardSelectionStrategy.ShardIdForExistingObject(entity);

			GetSingleShardSession(shardIds).Delete(entity);
		}

        public IRavenQueryable<T> Query<T>(string indexName)
	    {
	        throw new NotSupportedException("Sharded linq queries aren't supported currently");
	    }

		public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			throw new NotSupportedException("Sharded linq queries aren't supported currently");
		}

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

		public string StoreIdentifier
		{
			get
			{
				return "ShardedSession";
			}
		}

		#region IDisposable Members

		public void Dispose()
		{
			foreach (var shardSession in shardSessions)
				shardSession.Dispose();

			//dereference all event listeners
			Stored = null;
		    OnEntityConverted = null;
		}

		#endregion

		public DocumentConvention Conventions
		{
			get { throw new NotSupportedException("You cannot ask a sharded session for its conventions, internal sharded session may each have diffeernt conventions"); }
		}

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