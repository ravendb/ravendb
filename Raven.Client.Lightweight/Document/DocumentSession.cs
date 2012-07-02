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
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
#if !NET35
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Client.Document.Batches;
#endif
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Raven.Client.Util;

namespace Raven.Client.Document
{
#if !SILVERLIGHT
	/// <summary>
	/// Implements Unit of Work for accessing the RavenDB server
	/// </summary>
	public class DocumentSession : InMemoryDocumentSessionOperations, IDocumentSessionImpl, ITransactionalDocumentSession,
		ISyncAdvancedSessionOperation, IDocumentQueryGenerator
	{
#if !NET35
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private readonly List<ILazyOperation> pendingLazyOperations = new List<ILazyOperation>();
		private readonly Dictionary<ILazyOperation, Action<object>> onEvaluateLazy = new Dictionary<ILazyOperation, Action<object>>();
#endif
		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public IDatabaseCommands DatabaseCommands { get; private set; }

#if !NET35
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

		/// <summary>
		/// Access the eager operations
		/// </summary>
		public IEagerSessionOperations Eagerly
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
#if !NET35
, IAsyncDatabaseCommands asyncDatabaseCommands
#endif
)
			: base(documentStore, listeners, id)
		{
#if !NET35
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

#if !NET35

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
			return Lazily.Load<T>(ids, null);
		}

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Lazy<TResult> ILazySessionOperations.Load<TResult>(string id)
		{
			return Lazily.Load(id, (Action<TResult>)null);
		}

		/// <summary>
		/// Loads the specified ids and a function to call when it is evaluated
		/// </summary>
		public Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids, Action<TResult[]> onEval)
		{
			return LazyLoadInternal(ids.ToArray(), new string[0], onEval);
		}

		/// <summary>
		/// Loads the specified id and a function to call when it is evaluated
		/// </summary>
		public Lazy<TResult> Load<TResult>(string id, Action<TResult> onEval)
		{
			var lazyLoadOperation = new LazyLoadOperation<TResult>(id, new LoadOperation(this, DatabaseCommands.DisableAllCaching, id));
			return AddLazyOperation(lazyLoadOperation, onEval);
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
		Lazy<TResult> ILazySessionOperations.Load<TResult>(ValueType id, Action<TResult> onEval)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(TResult), false);
			return Lazily.Load<TResult>(documentKey);
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
			return Lazily.Load<T>(id, null);
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
			if (id == null) throw new ArgumentNullException("id", "The document id cannot be null");
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
			return LoadInternal<T>(ids);
		}

		/// <summary>
		/// Loads the specified entities with the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		public T[] Load<T>(IEnumerable<string> ids)
		{
			return ((IDocumentSessionImpl)this).LoadInternal<T>(ids.ToArray());
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

		public T[] LoadInternal<T>(string[] ids, string[] includes)
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

		public T[] LoadInternal<T>(string[] ids)
		{
			if (ids.Length == 0)
				return new T[0];

			// only load documents that aren't already cached
			var idsOfNotExistingObjects = ids.Where(id => IsLoaded(id) == false)
				.Distinct(StringComparer.InvariantCultureIgnoreCase)
				.ToArray();

			if (idsOfNotExistingObjects.Length > 0)
			{
				IncrementRequestCount();
				var multiLoadOperation = new MultiLoadOperation(this, DatabaseCommands.DisableAllCaching, idsOfNotExistingObjects);
				MultiLoadResult multiLoadResult;
				do
				{
					multiLoadOperation.LogOperation();
					using (multiLoadOperation.EnterMultiLoadContext())
					{
						multiLoadResult = DatabaseCommands.Get(idsOfNotExistingObjects, null);
					}
				} while (multiLoadOperation.SetResult(multiLoadResult));

				multiLoadOperation.Complete<T>();
			}

			return ids.Select(id =>
			{
				object val;
				entitiesByKey.TryGetValue(id, out val);
				return (T)val;
			}).ToArray();
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
#if !NET35
, AsyncDatabaseCommands
#endif
), ravenQueryStatistics, indexName, null, this, DatabaseCommands
#if !NET35
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
				throw new InvalidOperationException("Cannot refresh a transient instance");
			IncrementRequestCount();
			var jsonDocument = DatabaseCommands.Get(value.Key);
			if (jsonDocument == null)
				throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");

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

		protected override string GenerateKey(object entity)
		{
			return Conventions.GenerateDocumentKey(DatabaseCommands, entity);
		}

#if !NET35
		protected override Task<string> GenerateKeyAsync(object entity)
		{
			return Conventions.GenerateDocumentKeyAsync(AsyncDatabaseCommands, entity);
		}
#endif


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


		public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path)
		{
			return new MultiLoaderWithInclude<T>(this).Include<TInclude>(path);
		}

		/// <summary>
		/// Gets the document URL for the specified entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public string GetDocumentUrl(object entity)
		{
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
				throw new InvalidOperationException("Could not figure out identifier for transient instance");

			return DatabaseCommands.UrlFor(value.Key);
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

				var batchResults = DatabaseCommands.Batch(data.Commands);
				UpdateBatchResults(batchResults, data);
			}
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
#if !NET35
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
		/// Promotes a transaction specified to a distributed transaction
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns>The token representing the distributed transaction</returns>
		public override byte[] PromoteTransaction(Guid fromTxId)
		{
			IncrementRequestCount();
			return DatabaseCommands.PromoteTransaction(fromTxId);
		}

		/// <summary>
		/// Stores the recovery information for the specified transaction
		/// </summary>
		/// <param name="resourceManagerId">The resource manager Id for this transaction</param>
		/// <param name="txId">The tx id.</param>
		/// <param name="recoveryInformation">The recovery information.</param>
		public void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation)
		{
			IncrementRequestCount();
			DatabaseCommands.StoreRecoveryInformation(resourceManagerId, txId, recoveryInformation);
		}

		/// <summary>
		/// Query RavenDB dynamically using LINQ
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		public IRavenQueryable<T> Query<T>()
		{
			var indexName = "dynamic";
			if (typeof(T).IsEntityType())
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
			return Query<T>(indexName);
		}

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		public IDocumentQuery<T> LuceneQuery<T>()
		{
			string indexName = "dynamic";
			if (typeof(T).IsEntityType())
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
			return Advanced.LuceneQuery<T>(indexName);
		}

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName)
		{
			return Advanced.LuceneQuery<T>(indexName);
		}

#if !NET35

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IAsyncDocumentQuery<T> IDocumentQueryGenerator.AsyncQuery<T>(string indexName)
		{
			throw new NotSupportedException();
		}

		internal Lazy<T> AddLazyOperation<T>(ILazyOperation operation, Action<T> onEval)
		{
			pendingLazyOperations.Add(operation);
			var lazyValue = new Lazy<T>(() =>
			{
				ExecuteAllPendingLazyOperations();
				return (T)operation.Result;
			});

			if (onEval != null)
				onEvaluateLazy[operation] = theResult => onEval((T)theResult);

			return lazyValue;
		}

		/// <summary>
		/// Register to lazily load documents and include
		/// </summary>
		public Lazy<T[]> LazyLoadInternal<T>(string[] ids, string[] includes, Action<T[]> onEval)
		{
			var multiLoadOperation = new MultiLoadOperation(this, DatabaseCommands.DisableAllCaching, ids);
			var lazyOp = new LazyMultiLoadOperation<T>(multiLoadOperation, ids, includes);
			return AddLazyOperation(lazyOp, onEval);
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

				foreach (var pendingLazyOperation in pendingLazyOperations)
				{
					Action<object> value;
					if (onEvaluateLazy.TryGetValue(pendingLazyOperation, out value))
						value(pendingLazyOperation.Result);
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
				if(DatabaseCommands is ServerClient) // server mode
				{
					var requests = pendingLazyOperations.Select(x => x.CraeteRequest()).ToArray();
					var responses = DatabaseCommands.MultiGet(requests);
					for (int i = 0; i < pendingLazyOperations.Count; i++)
					{
						if (responses[i].RequestHasErrors())
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
				else // embedded mode
				{
					var responses = pendingLazyOperations.Select(x => x.ExecuteEmbedded(DatabaseCommands)).ToArray();
					for (int i = 0; i < pendingLazyOperations.Count; i++)
					{
						pendingLazyOperations[i].HandleEmbeddedResponse(responses[i]);
						if (pendingLazyOperations[i].RequiresRetry)
						{
							return true;
						}
					}
					return false;
					
				}
				
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
		public IEnumerable<T> LoadStartingWith<T>(string keyPrefix, int start = 0, int pageSize = 25)
		{
			return DatabaseCommands.StartsWith(keyPrefix, start, pageSize).Select(TrackEntity<T>).ToList();
		}
	}
#endif
}