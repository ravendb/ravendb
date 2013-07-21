//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Util;
using Raven.Json.Linq;

namespace Raven.Client.Document.Async
{
	using Linq;

	/// <summary>
	/// Implementation for async document session 
	/// </summary>
	public class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
	{
		private readonly AsyncDocumentKeyGeneration asyncDocumentKeyGeneration;

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncDocumentSession"/> class.
		/// </summary>
		public AsyncDocumentSession(string dbName, DocumentStore documentStore,
									IAsyncDatabaseCommands asyncDatabaseCommands,
									DocumentSessionListeners listeners,
									Guid id)
			: base(dbName, documentStore, listeners, id)
		{
			AsyncDatabaseCommands = asyncDatabaseCommands;
			GenerateDocumentKeysOnStore = false;
			asyncDocumentKeyGeneration = new AsyncDocumentKeyGeneration(this, entitiesAndMetadata.TryGetValue, (key, entity, metadata) => key);
		}

		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		public IAsyncDatabaseCommands AsyncDatabaseCommands { get; private set; }

		/// <summary>
		/// Load documents with the specified key prefix
		/// </summary>
		public Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, int start = 0, int pageSize = 25)
		{
			return AsyncDatabaseCommands.StartsWithAsync(keyPrefix, start, pageSize)
										.ContinueWith(task => (IEnumerable<T>)task.Result.Select(TrackEntity<T>).ToList());
		}

		public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query)
		{
			return StreamAsync(query, new Reference<QueryHeaderInformation>());
		}

		public Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query)
		{
			return StreamAsync(query, new Reference<QueryHeaderInformation>());
		}


		public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformation)
		{
			var queryInspector = (IRavenQueryProvider)query.Provider;
			var indexQuery = queryInspector.ToAsyncLuceneQuery<T>(query.Expression);
			return await StreamAsync(indexQuery, queryHeaderInformation);
		}

		public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation)
		{
			var ravenQueryInspector = ((IRavenQueryInspector)query);
			var indexQuery = ravenQueryInspector.GetIndexQuery(true);
			var enumerator = await AsyncDatabaseCommands.StreamQueryAsync(ravenQueryInspector.AsyncIndexQueried, indexQuery, queryHeaderInformation);
			var queryOperation = ((AsyncDocumentQuery<T>)query).InitializeQueryOperation(null);
			queryOperation.DisableEntitiesTracking = true;

			return new QueryYieldStream<T>(this, enumerator, queryOperation);
		}

		public async Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0,
								   int pageSize = Int32.MaxValue)
		{
			var enumerator = await AsyncDatabaseCommands.StreamDocsAsync(fromEtag, startsWith, matches, start, pageSize);
			return new DocsYieldStream<T>(this, enumerator);
		}

		public abstract class YieldStream<T> : IAsyncEnumerator<StreamResult<T>>
		{
			protected readonly AsyncDocumentSession parent;
			protected readonly IAsyncEnumerator<RavenJObject> enumerator;

			protected YieldStream(AsyncDocumentSession parent, IAsyncEnumerator<RavenJObject> enumerator)
			{
				this.parent = parent;
				this.enumerator = enumerator;
			}

			public void Dispose()
			{
				enumerator.Dispose();
			}

			public async Task<bool> MoveNextAsync()
			{
				if (await enumerator.MoveNextAsync() == false)
					return false;

				SetCurrent();

				return true;
			}

			protected abstract void SetCurrent();

			public StreamResult<T> Current { get; protected set; }
		}
		public class QueryYieldStream<T> : YieldStream<T>
		{
			private readonly QueryOperation queryOperation;

			public QueryYieldStream(AsyncDocumentSession parent, IAsyncEnumerator<RavenJObject> enumerator, QueryOperation queryOperation)
				: base(parent, enumerator)
			{
				this.queryOperation = queryOperation;
			}

			protected override void SetCurrent()
			{
				var meta = enumerator.Current.Value<RavenJObject>(Constants.Metadata);

				string key = null;
				Etag etag = null;
				if (meta != null)
				{
					key = meta.Value<string>(Constants.DocumentIdFieldName);
					var value = meta.Value<string>("@etag");
					if (value != null)
						etag = Etag.Parse(value);
				}

				Current = new StreamResult<T>
				{
					Document = queryOperation.Deserialize<T>(enumerator.Current),
					Etag = etag,
					Key = key,
					Metadata = meta
				};
			}
		}

		public class DocsYieldStream<T> : YieldStream<T>
		{
			public DocsYieldStream(AsyncDocumentSession parent, IAsyncEnumerator<RavenJObject> enumerator)
				: base(parent, enumerator)
			{
			}

			protected override void SetCurrent()
			{
				var document = SerializationHelper.RavenJObjectToJsonDocument(enumerator.Current);

				Current = new StreamResult<T>
				{
					Document = (T)parent.ConvertToEntity<T>(document.Key, document.DataAsJson, document.Metadata),
					Etag = document.Etag,
					Key = document.Key,
					Metadata = document.Metadata
				};
			}
		}
		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index, bool isMapReduce)
		{
			return new AsyncDocumentQuery<T>(this,
#if !SILVERLIGHT
 null,
#endif
 AsyncDatabaseCommands, index, new string[0], new string[0], listeners.QueryListeners, isMapReduce);
		}

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>()
		{
			var indexName = "dynamic";
			if (typeof(T).IsEntityType())
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
			return new AsyncDocumentQuery<T>(this,
#if !SILVERLIGHT
 null,
#endif
 AsyncDatabaseCommands, indexName, new string[0], new string[0], listeners.QueryListeners, false);
		}

		/// <summary>
		/// Get the accessor for advanced operations
		/// </summary>
		/// <remarks>
		/// Those operations are rarely needed, and have been moved to a separate 
		/// property to avoid cluttering the API
		/// </remarks>
		public IAsyncAdvancedSessionOperations Advanced
		{
			get { return this; }
		}

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		public IAsyncLoaderWithInclude<object> Include(string path)
		{
			return new AsyncMultiLoaderWithInclude<object>(this).Include(path);
		}

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		public IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
		{
			return new AsyncMultiLoaderWithInclude<T>(this).Include(path);
		}

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		public IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path)
		{
			return new AsyncMultiLoaderWithInclude<T>(this).Include<TInclude>(path);
		}

		/// <summary>
		/// Begins the async load operation, with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(1)
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public Task<T> LoadAsync<T>(ValueType id)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return LoadAsync<T>(documentKey);
		}

		/// <summary>
		/// Begins the async multi-load operation, with the specified ids after applying
		/// conventions on the provided ids to get the real document ids.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(1,2,3)
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public Task<T[]> LoadAsync<T>(params ValueType[] ids)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return LoadAsync<T>(documentKeys);
		}

		/// <summary>
		/// Begins the async multi-load operation, with the specified ids after applying
		/// conventions on the provided ids to get the real document ids.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public Task<T[]> LoadAsync<T>(IEnumerable<ValueType> ids)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return LoadAsync<T>(documentKeys);
		}

		/// <summary>
		/// Begins the async load operation
		/// </summary>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		public Task<T> LoadAsync<T>(string id)
		{
			if (id == null) throw new ArgumentNullException("id", "The document id cannot be null");
			object entity;
			if (entitiesByKey.TryGetValue(id, out entity))
			{
				var tcs = new TaskCompletionSource<T>();
				tcs.TrySetResult((T)entity);
				return tcs.Task;
			}
			IncrementRequestCount();
			var loadOperation = new LoadOperation(this, AsyncDatabaseCommands.DisableAllCaching, id);
			return CompleteLoadAsync<T>(id, loadOperation);
		}

		private Task<T> CompleteLoadAsync<T>(string id, LoadOperation loadOperation)
		{
			loadOperation.LogOperation();
			using (loadOperation.EnterLoadContext())
			{
				return AsyncDatabaseCommands.GetAsync(id)
											.ContinueWith(task =>
											{
												if (loadOperation.SetResult(task.Result) == false)
													return Task.Factory.StartNew(() => loadOperation.Complete<T>());

												return CompleteLoadAsync<T>(id, loadOperation);
											})
											.Unwrap();
			}
		}

		/// <summary>
		/// Begins the async multi load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		public Task<T[]> LoadAsync<T>(params string[] ids)
		{
			return LoadAsyncInternal<T>(ids, new KeyValuePair<string, Type>[0]);
		}

		public Task<T[]> LoadAsync<T>(IEnumerable<string> ids)
		{
			return LoadAsyncInternal<T>(ids.ToArray(), new KeyValuePair<string, Type>[0]);
		}

		public async Task<T> LoadAsync<TTransformer, T>(string id) where TTransformer : AbstractTransformerCreationTask, new()
		{
			var transformer = new TTransformer();
			var result = await LoadAsyncInternal<T>(new[] { id }, new KeyValuePair<string, Type>[0], transformer.TransformerName);
			return result.FirstOrDefault();
		}

		public async Task<T> LoadAsync<TTransformer, T>(string id, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
		{
			var transformer = new TTransformer();
			var ravenLoadConfiguration = new RavenLoadConfiguration();
			configure(ravenLoadConfiguration);
			var result = await LoadAsyncInternal<T>(new[] { id }, new KeyValuePair<string, Type>[0], transformer.TransformerName, ravenLoadConfiguration.QueryInputs);
			return result.FirstOrDefault();
		}

		public async Task<TResult[]> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
		{
			var transformer = new TTransformer();
			var ravenLoadConfiguration = new RavenLoadConfiguration();
			configure(ravenLoadConfiguration);
			var result = await LoadAsyncInternal<TResult>(ids.ToArray(), new KeyValuePair<string, Type>[0], transformer.TransformerName, ravenLoadConfiguration.QueryInputs);
			return result;
		}

		public Task<T[]> LoadAsync<TTransformer, T>(params string[] ids) where TTransformer : AbstractTransformerCreationTask, new()
		{
			var transformer = new TTransformer();
			return LoadAsyncInternal<T>(ids, new KeyValuePair<string, Type>[0], transformer.TransformerName);
		}

		/// <summary>
		/// Begins the async multi load operation
		/// </summary>
		public async Task<T[]> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transfomer = null, Dictionary<string, RavenJToken> queryInputs = null)
		{
			IncrementRequestCount();
			var multiLoadOperation = new MultiLoadOperation(this, AsyncDatabaseCommands.DisableAllCaching, ids, includes);

			multiLoadOperation.LogOperation();
			var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
			MultiLoadResult result;
			do
			{
				multiLoadOperation.LogOperation();
				using (multiLoadOperation.EnterMultiLoadContext())
				{
					result = await AsyncDatabaseCommands.GetAsync(ids, includePaths, transfomer, queryInputs);
				}
			} while (multiLoadOperation.SetResult(result));
			return multiLoadOperation.Complete<T>();
		}

		/// <summary>
		/// Begins the async save changes operation
		/// </summary>
		/// <returns></returns>
		public Task SaveChangesAsync()
		{

			return asyncDocumentKeyGeneration.GenerateDocumentKeysForSaveChanges()
											 .ContinueWith(keysTask =>
											 {
												 keysTask.AssertNotFailed();

												 var cachingScope = EntityToJson.EntitiesToJsonCachingScope();
												 try
												 {
													 var data = PrepareForSaveChanges();
													 if (data.Commands.Count == 0)
													 {
														 cachingScope.Dispose();
														 return new CompletedTask();
													 }

													 IncrementRequestCount();

													 return AsyncDatabaseCommands.BatchAsync(data.Commands.ToArray())
																				 .ContinueWith(task =>
																				 {
																					 try
																					 {
																						 UpdateBatchResults(task.Result, data);
																					 }
																					 finally
																					 {
																						 cachingScope.Dispose();
																					 }
																				 });
												 }
												 catch
												 {
													 cachingScope.Dispose();
													 throw;
												 }
											 }).Unwrap();
		}

		/// <summary>
		/// Get the json document by key from the store
		/// </summary>
		protected override JsonDocument GetJsonDocument(string documentKey)
		{
			throw new NotSupportedException("Cannot get a document in a synchronous manner using async document session");
		}

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public override void Commit(string txId)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public override void Rollback(string txId)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Dynamically queries RavenDB using LINQ
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		public IRavenQueryable<T> Query<T>()
		{
			string indexName = "dynamic";
			if (typeof(T).IsEntityType())
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}

			return Query<T>(indexName);
		}

		public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var indexCreator = new TIndexCreator();
			return Query<T>(indexCreator.IndexName, indexCreator.IsMapReduce);
		}

		public IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false)
		{
			var ravenQueryStatistics = new RavenQueryStatistics();
			var highlightings = new RavenQueryHighlightings();
			return new RavenQueryInspector<T>(
				new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, highlightings,
#if !SILVERLIGHT
 null,
#endif
 AsyncDatabaseCommands, isMapReduce),
				ravenQueryStatistics,
				highlightings,
				indexName,
				null,
				this,
#if !SILVERLIGHT
 null,
#endif
 AsyncDatabaseCommands,
				isMapReduce);
		}

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
		{
			throw new NotSupportedException("You can't get a sync query from async session");
		}

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		public IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, bool isMapReduce = false)
		{
			return AsyncLuceneQuery<T>(indexName, isMapReduce);
		}

		protected override string GenerateKey(object entity)
		{
			throw new NotSupportedException("Async session cannot generate keys synchronously");
		}

		protected override void RememberEntityForDocumentKeyGeneration(object entity)
		{
			asyncDocumentKeyGeneration.Add(entity);
		}

		protected override Task<string> GenerateKeyAsync(object entity)
		{
			return Conventions.GenerateDocumentKeyAsync(dbName, AsyncDatabaseCommands, entity);
		}
	}
}
