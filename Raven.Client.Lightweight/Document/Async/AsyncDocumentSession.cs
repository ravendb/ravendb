//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET35

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Util;

namespace Raven.Client.Document.Async
{
	using Linq;

	/// <summary>
	/// Implementation for async document session 
	/// </summary>
	public class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
	{
		private AsyncDocumentKeyGeneration asyncDocumentKeyGeneration;

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncDocumentSession"/> class.
		/// </summary>
		public AsyncDocumentSession(DocumentStore documentStore,
			IAsyncDatabaseCommands asyncDatabaseCommands,
			DocumentSessionListeners listeners,
			Guid id)
			: base(documentStore, listeners, id)
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

		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index)
		{
			return new AsyncDocumentQuery<T>(this,
#if !SILVERLIGHT
				null,
#endif
				AsyncDatabaseCommands, index, new string[0], listeners.QueryListeners);
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
				AsyncDatabaseCommands, indexName, new string[0], listeners.QueryListeners);
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
		public Task<T> LoadAsync<T>(ValueType id)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return LoadAsync<T>(documentKey);
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
		public Task<T[]> LoadAsync<T>(string[] ids)
		{
			return LoadAsyncInternal<T>(ids, new string[0]);
		}


		/// <summary>
		/// Begins the async multi load operation
		/// </summary>
		public Task<T[]> LoadAsyncInternal<T>(string[] ids, string[] includes)
		{
			IncrementRequestCount();
			var multiLoadOperation = new MultiLoadOperation(this, AsyncDatabaseCommands.DisableAllCaching, ids);
			return LoadAsyncInternal<T>(ids, includes, multiLoadOperation);
		}

		private Task<T[]> LoadAsyncInternal<T>(string[] ids, string[] includes, MultiLoadOperation multiLoadOperation)
		{
			multiLoadOperation.LogOperation();
			using (multiLoadOperation.EnterMultiLoadContext())
			{
				return AsyncDatabaseCommands.GetAsync(ids, includes)
					.ContinueWith(t =>
					{
						if (multiLoadOperation.SetResult(t.Result) == false)
							return Task.Factory.StartNew(() => multiLoadOperation.Complete<T>());
						return LoadAsyncInternal<T>(ids, includes, multiLoadOperation);
					})
					.Unwrap();
			}
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

					var cachingScope = EntitiesToJsonCachingScope();
					try
					{
						var data = PrepareForSaveChanges();
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
		public override void Commit(Guid txId)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public override void Rollback(Guid txId)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Promotes the transaction.
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns></returns>
		public override byte[] PromoteTransaction(Guid fromTxId)
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
			return Query<T>(new TIndexCreator().IndexName);
		}

		public IRavenQueryable<T> Query<T>(string indexName)
		{
			var ravenQueryStatistics = new RavenQueryStatistics();

			return new RavenQueryInspector<T>(
				new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics,
#if !SILVERLIGHT
				null,
#endif
			AsyncDatabaseCommands),
				ravenQueryStatistics,
				indexName,
				null,
				this,
#if !SILVERLIGHT
				null,
#endif
				AsyncDatabaseCommands);
		}

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName)
		{
			throw new NotSupportedException("You can't get a sync query from async session");
		}

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		public IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName)
		{
			return AsyncLuceneQuery<T>(indexName);
		}

		protected override void RememberEntityForDocumentKeyGeneration(object entity)
		{
			asyncDocumentKeyGeneration.Add(entity);
		}
	}
}
#endif
