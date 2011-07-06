//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System;
using System.Text;
using System.Threading;
using Raven.Abstractions.Data;
#if !NET_3_5
using Raven.Client.Connection.Async;
using Raven.Client.Document.Batches;
#endif
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Listeners;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
#if !SILVERLIGHT
	/// <summary>
	/// Implements Unit of Work for accessing the RavenDB server
	/// </summary>
	public class DocumentSession : InMemoryDocumentSessionOperations, IDocumentSession, ITransactionalDocumentSession, ISyncAdvancedSessionOperation, IDocumentQueryGenerator
#if !NET_3_5
		, ILazySessionOperations
#endif
	{
#if !NET_3_5
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private readonly List<ILazyOperation> pendingLazyOperations = new List<ILazyOperation>();
#endif
		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public IDatabaseCommands DatabaseCommands { get; private set; }

#if !NET_3_5
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return asyncDatabaseCommands; }
		}

		/// <summary>
		/// Access the lazy operations
		/// </summary>
		public ILazySessionOperations Lazily
		{
			get { return this; }
		}
#endif

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentSession"/> class.
		/// </summary>
		public DocumentSession(DocumentStore documentStore,
			DocumentSessionListeners listeners,
			Guid id,
			IDatabaseCommands databaseCommands
#if !NET_3_5
, IAsyncDatabaseCommands asyncDatabaseCommands
#endif
)
			: base(documentStore, listeners, id)
		{
#if !NET_3_5
			this.asyncDatabaseCommands = asyncDatabaseCommands;
#endif
			DatabaseCommands = databaseCommands;
		}

		/// <summary>
		/// Get the accessor for advanced operations
		/// </summary>
		/// <remarks>
		/// Those operations are rarely needed, and have been moved to a separate 
		/// property to avoid cluttering the API
		/// </remarks>
		public ISyncAdvancedSessionOperation Advanced
		{
			get { return this; }
		}

#if !NET_3_5

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, object>> path)
		{
			return new LazyMultiLoaderWithInclude<T>(this).Include(path);
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Lazy<T[]> ILazySessionOperations.Load<T>(params string[] ids)
		{
			return LazyLoadInternal<T>(ids, new string[0]);
		}

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Lazy<T> ILazySessionOperations.Load<T>(string id)
		{
			var lazyLoadOperation = new LazyLoadOperation<T>(id, new LoadOperation(this, DatabaseCommands.DisableAllCaching, id));
			return AddLazyOperation<T>(lazyLoadOperation);
		}


		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
		{
			return new LazyMultiLoaderWithInclude<object>(this).Include(path);
		}

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Lazy<T> ILazySessionOperations.Load<T>(ValueType id)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return Lazily.Load<T>(documentKey);
		}
#endif

		/// <summary>
		/// Loads the specified entity with the specified id.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		public T Load<T>(string id)
		{
			object existingEntity;
			if (entitiesByKey.TryGetValue(id, out existingEntity))
			{
				return (T)existingEntity;
			}

			IncrementRequestCount();
			var loadOperation = new LoadOperation(this, DatabaseCommands.DisableAllCaching, id);
			bool retry;
			do
			{
				loadOperation.LogOperation();
				using (loadOperation.EnterLoadContext())
				{
					retry = loadOperation.SetResult(DatabaseCommands.Get(id));
				}
			} while (retry);
			return loadOperation.Complete<T>();
		}

		/// <summary>
		/// Loads the specified entities with the specified ids.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		public T[] Load<T>(params string[] ids)
		{
			return LoadInternal<T>(ids, null);
		}

		/// <summary>
		/// Loads the specified entities with the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		public T[] Load<T>(IEnumerable<string> ids)
		{
			return LoadInternal<T>(ids.ToArray(), null);
		}

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public T Load<T>(ValueType id)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return Load<T>(documentKey);
		}

		internal T[] LoadInternal<T>(string[] ids, string[] includes)
		{
			if (ids.Length == 0)
				return new T[0];

			IncrementRequestCount();
			var multiLoadOperation = new MultiLoadOperation(this, DatabaseCommands.DisableAllCaching, ids);
			MultiLoadResult multiLoadResult;
			do
			{
				multiLoadOperation.LogOperation();
				using (multiLoadOperation.EnterMultiLoadContext())
				{
					multiLoadResult = DatabaseCommands.Get(ids, includes);
				}
			} while (multiLoadOperation.SetResult(multiLoadResult));

			return multiLoadOperation.Complete<T>();
		}

		/// <summary>
		/// Queries the specified index using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <param name="indexName">Name of the index.</param>
		/// <returns></returns>
		public IRavenQueryable<T> Query<T>(string indexName)
		{
			var ravenQueryStatistics = new RavenQueryStatistics();
			return new RavenQueryInspector<T>(new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, DatabaseCommands
#if !NET_3_5
, AsyncDatabaseCommands
#endif
), ravenQueryStatistics, indexName, null, DatabaseCommands
#if !NET_3_5
, AsyncDatabaseCommands
#endif
);
		}

		/// <summary>
		/// Queries the index specified by <typeparamref name="TIndexCreator"/> using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var indexCreator = new TIndexCreator();
			return Query<T>(indexCreator.IndexName);
		}

		/// <summary>
		/// Refreshes the specified entity from Raven server.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		public void Refresh<T>(T entity)
		{
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
				throw new InvalidOperationException("Cannot refresh a trasient instance");
			IncrementRequestCount();
			var jsonDocument = DatabaseCommands.Get(value.Key);
			if (jsonDocument == null)
				throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");

			value.Metadata = jsonDocument.Metadata;
			value.OriginalMetadata = (RavenJObject)jsonDocument.Metadata.CloneToken();
			value.ETag = jsonDocument.Etag;
			value.OriginalValue = jsonDocument.DataAsJson;
			var newEntity = ConvertToEntity<T>(value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
			foreach (PropertyInfo property in entity.GetType().GetProperties())
			{
				if (!property.CanWrite || !property.CanRead || property.GetIndexParameters().Length != 0)
					continue;
				property.SetValue(entity, property.GetValue(newEntity, null), null);
			}
		}

		/// <summary>
		/// Get the json document by key from the store
		/// </summary>
		protected override JsonDocument GetJsonDocument(string documentKey)
		{
			var jsonDocument = DatabaseCommands.Get(documentKey);
			if (jsonDocument == null)
				throw new InvalidOperationException("Document '" + documentKey + "' no longer exists and was probably deleted");
			return jsonDocument;
		}

		/// <summary>
		/// Begin a load while including the specified path
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		public ILoaderWithInclude<object> Include(string path)
		{
			return new MultiLoaderWithInclude<object>(this).Include(path);
		}

		/// <summary>
		/// Begin a load while including the specified path
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		public ILoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
		{
			return new MultiLoaderWithInclude<T>(this).Include(path);
		}

		/// <summary>
		/// Gets the document URL for the specified entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public string GetDocumentUrl(object entity)
		{
			if (string.IsNullOrEmpty(DocumentStore.Url))
				throw new InvalidOperationException("Could not provide document url for embedded instance");

			DocumentMetadata value;
			string baseUrl = DocumentStore.Url.EndsWith("/") ? DocumentStore.Url + "docs/" : DocumentStore.Url + "/docs/";
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
			{
				string id;
				TryGetIdFromInstance(entity, out id);
				if (string.IsNullOrEmpty(id))
					throw new InvalidOperationException("Could not figure out identifier for transient instance");
				return baseUrl + id;
			}

			return baseUrl + value.Key;
		}

		/// <summary>
		/// Saves all the changes to the Raven server.
		/// </summary>
		public void SaveChanges()
		{
			using (EntitiesToJsonCachingScope())
			{
				var data = PrepareForSaveChanges();
				if (data.Commands.Count == 0)
					return; // nothing to do here
				IncrementRequestCount();
				LogBatch(data);
				UpdateBatchResults(DatabaseCommands.Batch(data.Commands), data.Entities);
			}
		}

		private void LogBatch(SaveChangesData data)
		{
			log.Debug(()=>
			{
				var sb = new StringBuilder()
					.AppendFormat("Saving {0} changes to {1}", data.Commands.Count, StoreIdentifier)
					.AppendLine();
				foreach (var commandData in data.Commands)
				{
					sb.AppendFormat("\t{0} {1}", commandData.Method, commandData.Key).AppendLine();
				}
				return sb.ToString();
			});
		}


		/// <summary>
		/// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var index = new TIndexCreator();
			return LuceneQuery<T>(index.IndexName);
		}

		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="indexName">Name of the index.</param>
		/// <returns></returns>
		public IDocumentQuery<T> LuceneQuery<T>(string indexName)
		{
#if !NET_3_5
			return new DocumentQuery<T>(this, DatabaseCommands, null, indexName, null, listeners.QueryListeners);
#else
			return new DocumentQuery<T>(this, DatabaseCommands, indexName, null, listeners.QueryListeners);
#endif
		}

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public override void Commit(Guid txId)
		{
			IncrementRequestCount();
			DatabaseCommands.Commit(txId);
			ClearEnlistment();
		}

		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public override void Rollback(Guid txId)
		{
			IncrementRequestCount();
			DatabaseCommands.Rollback(txId);
			ClearEnlistment();
		}

		/// <summary>
		/// Promotes the transaction.
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns></returns>
		public override byte[] PromoteTransaction(Guid fromTxId)
		{
			IncrementRequestCount();
			return DatabaseCommands.PromoteTransaction(fromTxId);
		}

		/// <summary>
		/// Stores the recovery information for the specified transaction
		/// </summary>
		/// <param name="resourceManagerId"></param>
		/// <param name="txId">The tx id.</param>
		/// <param name="recoveryInformation">The recovery information.</param>
		public void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation)
		{
			IncrementRequestCount();
			DatabaseCommands.StoreRecoveryInformation(resourceManagerId, txId, recoveryInformation);
		}

		/// <summary>
		/// Dynamically queries RavenDB using LINQ
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		public IRavenQueryable<T> Query<T>()
		{
			string indexName = "dynamic";
			if (typeof(T) != typeof(object))
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

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		public IDocumentQuery<T> LuceneQuery<T>()
		{
			string indexName = "dynamic";
			if (typeof(T) != typeof(object))
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
			return LuceneQuery<T>(indexName);
		}

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName)
		{
			return Advanced.LuceneQuery<T>(indexName);
		}

#if !NET_3_5

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IAsyncDocumentQuery<T> IDocumentQueryGenerator.AsyncQuery<T>(string indexName)
		{
			throw new NotSupportedException();
		}

		internal Lazy<T> AddLazyOperation<T>(ILazyOperation operation)
		{
			pendingLazyOperations.Add(operation);
			return new Lazy<T>(() =>
			{
				ExecuteAllPendingLazyOperations();
				return (T)operation.Result;
			});
		}

		/// <summary>
		/// Register to lazily load documents and include
		/// </summary>
		public Lazy<T[]> LazyLoadInternal<T>(string[] ids, string[] includes)
		{
			var multiLoadOperation = new MultiLoadOperation(this, DatabaseCommands.DisableAllCaching, ids);
			var lazyOp = new LazyMultiLoadOperation<T>(multiLoadOperation, ids, includes);
			return AddLazyOperation<T[]>(lazyOp);
		}


		public void ExecuteAllPendingLazyOperations()
		{
			if (pendingLazyOperations.Count == 0)
				return;

			try
			{
				IncrementRequestCount();
				while (ExecuteLazyOperationsSingleStep())
				{
					Thread.Sleep(100);
				}
			}
			finally
			{
				pendingLazyOperations.Clear();
			}
		}

		private bool ExecuteLazyOperationsSingleStep()
		{
			var disposables = pendingLazyOperations.Select(x => x.EnterContext()).Where(x => x != null).ToList();
			try
			{
				var requests = pendingLazyOperations.Select(x => x.CraeteRequest()).ToArray();
				var responses = DatabaseCommands.MultiGet(requests);
				for (int i = 0; i < pendingLazyOperations.Count; i++)
				{
					if (responses[i].Status != 200 && // known statuses, with specific handling
						responses[i].Status != 203 &&
						responses[i].Status != 304 &&
						responses[i].Status != 404)
					{
						throw new InvalidOperationException("Got an error from server, status code: " + responses[i].Status +
															Environment.NewLine + responses[i].Result);
					}
					pendingLazyOperations[i].HandleResponse(responses[i]);
					if (pendingLazyOperations[i].RequiresRetry)
					{
						return true;
					}
				}
				return false;
			}
			finally
			{
				foreach (var disposable in disposables)
				{
					disposable.Dispose();
				}
			}
		}
#endif

	}
#endif
}
