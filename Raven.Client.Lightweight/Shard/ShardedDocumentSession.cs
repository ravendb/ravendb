//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;
#if !NET_3_5
#endif
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Document.Batches;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using System;
using Raven.Client.Util;
using Raven.Json.Linq;

namespace Raven.Client.Shard
{
#if !SILVERLIGHT
	/// <summary>
	/// Implements Unit of Work for accessing a set of sharded RavenDB servers
	/// </summary>
	public class ShardedDocumentSession : InMemoryDocumentSessionOperations, IDocumentSessionImpl, ITransactionalDocumentSession,
		ISyncAdvancedSessionOperation, IDocumentQueryGenerator
#if !NET_3_5
		, ILazySessionOperations, IEagerSessionOperations
#endif
	{
#if !NET_3_5
		//private readonly IDictionary<string, IAsyncDatabaseCommands> asyncShardDbCommands;
		private readonly List<ILazyOperation> pendingLazyOperations = new List<ILazyOperation>();
		private readonly Dictionary<ILazyOperation, Action<object>> onEvaluateLazy = new Dictionary<ILazyOperation, Action<object>>();
#endif
		private readonly IShardStrategy shardStrategy;
		private readonly IDictionary<string, IDatabaseCommands> shardDbCommands;
		private readonly ShardedDocumentStore documentStore;

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentSession"/> class.
		/// </summary>
		/// <param name="shardStrategy">The shard strategy.</param>
		/// <param name="shardDbCommands">The shard IDatabaseCommands.</param>
		/// <param name="id"></param>
		/// <param name="documentStore"></param>
		/// <param name="listeners"></param>
		public ShardedDocumentSession(ShardedDocumentStore documentStore, DocumentSessionListeners listeners, Guid id,
			IShardStrategy shardStrategy, IDictionary<string, IDatabaseCommands> shardDbCommands
//#if !NET_3_5
//, IDictionary<string, IAsyncDatabaseCommands> asyncDatabaseCommands
//#endif
			)
			: base(documentStore, listeners, id)
		{
			this.shardStrategy = shardStrategy;
			this.shardDbCommands = shardDbCommands;
			this.documentStore = documentStore;
//#if !NET_3_5
//            this.asyncShardDbCommands = asyncDatabaseCommands;
//#endif
		}

		private IList<IDatabaseCommands> GetAppropriateShards<T>(string key)
		{
			var shardIds = shardStrategy.ShardResolutionStrategy.SelectShardIds(ShardResolutionStrategyData.BuildFrom(typeof(T), key));
			
			if (shardIds != null)
				return shardDbCommands.Where(cmd => shardIds.Contains(cmd.Key)).Select(x => x.Value).ToList();
			
			return shardDbCommands.Values.ToList();
		}

		protected override JsonDocument GetJsonDocument(string documentKey)
		{
			var dbCommands = GetAppropriateShards<object>(documentKey);

			foreach (var dbCmd in dbCommands)
			{
				var jsonDocument = dbCmd.Get(documentKey);
				if (jsonDocument != null)
					return jsonDocument;
			}

			throw new InvalidOperationException("Document '" + documentKey + "' no longer exists and was probably deleted");
		}

		public override void Commit(Guid txId)
		{
			throw new NotImplementedException();
		}

		public override void Rollback(Guid txId)
		{
			throw new NotImplementedException();
		}

		public override byte[] PromoteTransaction(Guid fromTxId)
		{
			throw new NotImplementedException();
		}

		public void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation)
		{
			throw new NotImplementedException();
		}

		public ISyncAdvancedSessionOperation Advanced
		{
			get { return this; }
		}

		public Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids, Action<TResult[]> onEval)
		{
			throw new NotImplementedException();
		}

		Lazy<TResult> ILazySessionOperations.Load<TResult>(string id)
		{
			throw new NotImplementedException();
		}

		public Lazy<TResult> Load<TResult>(string id, Action<TResult> onEval)
		{
			throw new NotImplementedException();
		}

		Lazy<TResult> ILazySessionOperations.Load<TResult>(ValueType id)
		{
			throw new NotImplementedException();
		}

		public Lazy<TResult> Load<TResult>(ValueType id, Action<TResult> onEval)
		{
			throw new NotImplementedException();
		}

		public T Load<T>(string id)
		{
			object existingEntity;
			if (entitiesByKey.TryGetValue(id, out existingEntity))
			{
				return (T)existingEntity;
			}

			IncrementRequestCount();

			var dbCommands = GetAppropriateShards<T>(id);

			foreach (var dbCmd in dbCommands)
			{
				var loadOperation = new LoadOperation(this, dbCmd.DisableAllCaching, id);
				bool retry;
				do
				{
					loadOperation.LogOperation();
					using (loadOperation.EnterLoadContext())
					{
						retry = loadOperation.SetResult(dbCmd.Get(id));
					}
				} while (retry);
				var result = loadOperation.Complete<T>();
				
				if (!Equals(result, default(T)))
					return result;
			}

			return default(T);
		}

		ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, object>> path)
		{
			throw new NotImplementedException();
		}

		Lazy<TResult[]> ILazySessionOperations.Load<TResult>(params string[] ids)
		{
			throw new NotImplementedException();
		}

		public T[] Load<T>(params string[] ids)
		{
			return LoadInternal<T>(ids, null);
		}

		public T[] Load<T>(IEnumerable<string> ids)
		{
			throw new NotImplementedException();
		}

		public T Load<T>(ValueType id)
		{
			throw new NotImplementedException();
		}

		public IRavenQueryable<T> Query<T>(string indexName)
		{
			throw new NotImplementedException();
		}

		public IRavenQueryable<T> Query<T>()
		{
			throw new NotImplementedException();
		}

		public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			throw new NotImplementedException();
		}

		public ILoaderWithInclude<object> Include(string path)
		{
			return new MultiLoaderWithInclude<object>(this).Include(path);
		}

		ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
		{
			throw new NotImplementedException();
		}

		public ILoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
		{
			throw new NotImplementedException();
		}

		public void SaveChanges()
		{
			throw new NotImplementedException();
		}

		IAsyncDocumentQuery<T> IDocumentQueryGenerator.AsyncQuery<T>(string indexName)
		{
			throw new NotSupportedException("Shared document store doesn't support async operations");
		}

		IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName)
		{
			throw new NotImplementedException();
		}

		public void Refresh<T>(T entity)
		{
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
				throw new InvalidOperationException("Cannot refresh a trasient instance");
			IncrementRequestCount();

			var dbCommands = GetAppropriateShards<T>(null);
			foreach (var dbCmd in dbCommands)
			{
				var jsonDocument = dbCmd.Get(value.Key);
				if (jsonDocument == null)
					continue;

				value.Metadata = jsonDocument.Metadata;
				value.OriginalMetadata = (RavenJObject)jsonDocument.Metadata.CloneToken();
				value.ETag = jsonDocument.Etag;
				value.OriginalValue = jsonDocument.DataAsJson;
				var newEntity = ConvertToEntity<T>(value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
				foreach (var property in entity.GetType().GetProperties())
				{
					if (!property.CanWrite || !property.CanRead || property.GetIndexParameters().Length != 0)
						continue;
					property.SetValue(entity, property.GetValue(newEntity, null), null);
				}
			}

			throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");
		}

		public IDatabaseCommands DatabaseCommands
		{
			get { throw new NotSupportedException("Not supported in a sharded session"); }
		}

#if !NET_3_5
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { throw new NotSupportedException("Not supported in a sharded session"); }
		}

		/// <summary>
		/// Access the lazy operations
		/// </summary>
		public ILazySessionOperations Lazily
		{
			get { return this; }
		}

		/// <summary>
		/// Access the eager operations
		/// </summary>
		public IEagerSessionOperations Eagerly
		{
			get { return this; }
		}
#endif

		public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			throw new NotImplementedException();
		}

		public IDocumentQuery<T> LuceneQuery<T>(string indexName)
		{
			throw new NotImplementedException();
		}

		public IDocumentQuery<T> LuceneQuery<T>()
		{
			throw new NotImplementedException();
		}

		public string GetDocumentUrl(object entity)
		{
			throw new NotImplementedException();
		}

		public T[] LoadInternal<T>(string[] ids, string[] includes)
		{
			if (ids.Length == 0)
				return new T[0];

			var idsAndShards = ids.Select(id => new { id, urls = GetAppropriateShards<T>(id) })
				.GroupBy(x => x.urls, new DbCmdsListComparer());

			IncrementRequestCount();

			var multiLoadOperation = new MultiLoadOperation(this);
			foreach (var endpoint in idsAndShards)
			{
				var idsForShard = endpoint.Select(x => x.id).ToArray();
				multiLoadOperation.ids = idsForShard;

				foreach (var dbCmd in endpoint.Key)
				{
					multiLoadOperation.disableAllCaching = dbCmd.DisableAllCaching;
					MultiLoadResult multiLoadResult;
					do
					{
						multiLoadOperation.LogOperation();
						using (multiLoadOperation.EnterMultiLoadContext())
						{
							multiLoadResult = dbCmd.Get(idsForShard, includes);
						}
					} while (multiLoadOperation.SetResult(multiLoadResult));
				}
			}
			return multiLoadOperation.Complete<T>();
		}

		internal class DbCmdsListComparer : IEqualityComparer<IList<IDatabaseCommands>>
		{
			public bool Equals(IList<IDatabaseCommands> x, IList<IDatabaseCommands> y)
			{
				if (x.Count != y.Count)
					return false;

				return !x.Where((t, i) => t != y[i]).Any();
			}

			public int GetHashCode(IList<IDatabaseCommands> obj)
			{
				return obj.Aggregate(obj.Count, (current, item) => (current * 397) ^ item.GetHashCode());
			}

		}

		public void ExecuteAllPendingLazyOperations()
		{
			throw new NotImplementedException();
		}
	}
#endif
}
