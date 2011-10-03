//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

namespace Raven.Client.Shard
{
#if !SILVERLIGHT
	/// <summary>
	/// Implements Unit of Work for accessing a set of sharded RavenDB servers
	/// </summary>
	public class ShardedDocumentSession : InMemoryDocumentSessionOperations, IDocumentSession, IDocumentQueryGenerator, ISyncAdvancedSessionOperation
	{
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
			IShardStrategy shardStrategy, IDictionary<string, IDatabaseCommands> shardDbCommands)
			: base(documentStore, listeners, id)
		{
			this.shardStrategy = shardStrategy;
			this.shardDbCommands = shardDbCommands;
			this.documentStore = documentStore;
		}

		private IEnumerable<IDatabaseCommands> GetAppropriateShards<T>(string key)
		{
			var shardIds = shardStrategy.ShardResolutionStrategy.SelectShardIds(ShardResolutionStrategyData.BuildFrom(typeof(T), key));
			
			if (shardIds != null)
				return shardDbCommands.Where(cmd => shardIds.Contains(cmd.Key)).Select(x => x.Value);
			
			return shardDbCommands.Values;
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

		public ISyncAdvancedSessionOperation Advanced
		{
			get { return this; }
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

		public T[] Load<T>(params string[] ids)
		{
			throw new NotImplementedException();
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
			var indexName = "dynamic";
			if (typeof(T).IsEntityType())
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
			var ravenQueryStatistics = new RavenQueryStatistics();
			return new RavenQueryInspector<T>(
				new DynamicRavenQueryProvider<T>(this, indexName, ravenQueryStatistics, Advanced.DatabaseCommands
#if !NET_3_5
, Advanced.AsyncDatabaseCommands
#endif
),
				ravenQueryStatistics,
				indexName,
				null,
				Advanced.DatabaseCommands
#if !NET_3_5
, Advanced.AsyncDatabaseCommands
#endif
);
		}

		public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			throw new NotImplementedException();
		}

		public ILoaderWithInclude<object> Include(string path)
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
			throw new NotImplementedException();
		}

		IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName)
		{
			throw new NotImplementedException();
		}

		public void Refresh<T>(T entity)
		{
			throw new NotImplementedException();
		}

		public IDatabaseCommands DatabaseCommands
		{
			get { throw new NotImplementedException(); }
		}

		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { throw new NotImplementedException(); }
		}

		public ILazySessionOperations Lazily
		{
			get { throw new NotImplementedException(); }
		}

		public IEagerSessionOperations Eagerly
		{
			get { throw new NotImplementedException(); }
		}

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
	}
#endif
}
