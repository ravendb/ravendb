using System;
using System.Linq;
using System.Net;
using Raven.Client.Client;
using Raven.Client.Client.Async;
using Raven.Database;

namespace Raven.Client.Document.Async
{
	/// <summary>
	/// Implementation for async document session 
	/// </summary>
	public class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSession, IAsyncAdvancedSessionOperations
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncDocumentSession"/> class.
		/// </summary>
		/// <param name="documentStore">The document store.</param>
		/// <param name="storeListeners">The store listeners.</param>
		/// <param name="deleteListeners">The delete listeners.</param>
		public AsyncDocumentSession(DocumentStore documentStore, IDocumentStoreListener[] storeListeners, IDocumentDeleteListener[] deleteListeners)
			: base(documentStore, storeListeners, deleteListeners)
		{
			AsyncDatabaseCommands = documentStore.AsyncDatabaseCommands;
		}

		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		public IAsyncDatabaseCommands AsyncDatabaseCommands { get; private set; }

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
		/// Begins the aysnc load operation
		/// </summary>
		/// <param name="id">The id.</param>
		/// <param name="asyncCallback">The async callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		public IAsyncResult BeginLoad(string id, AsyncCallback asyncCallback, object state)
		{
			object entity;
			if (entitiesByKey.TryGetValue(id, out entity))
				return new SyncronousLoadResult(state, entity);
			
			IncrementRequestCount();

			return AsyncDatabaseCommands.BeginGet(id, asyncCallback, state);
		}

		/// <summary>
		/// Ends the async load operation
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="result">The result.</param>
		/// <returns></returns>
		public T EndLoad<T>(IAsyncResult result)
		{
			var syncronousLoadResult = result as SyncronousLoadResult;
			if (syncronousLoadResult != null)
				return (T) syncronousLoadResult.Entity;

			JsonDocument documentFound;
			try
			{
				documentFound = AsyncDatabaseCommands.EndGet(result);
			}
			catch (WebException ex)
			{
				var httpWebResponse = ex.Response as HttpWebResponse;
				if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					return default(T);
				throw;
			}
			if (documentFound == null)
				return default(T);

			return TrackEntity<T>(documentFound);
		}

		/// <summary>
		/// Begins the async multi load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <param name="asyncCallback">The async callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		public IAsyncResult BeginMultiLoad(string[] ids, AsyncCallback asyncCallback, object state)
		{
			IncrementRequestCount();
			return AsyncDatabaseCommands.BeginMultiGet(ids, asyncCallback, state);
		}

		/// <summary>
		/// Ends the async multi load operation
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="result">The result.</param>
		/// <returns></returns>
		public T[] EndMultiLoad<T>(IAsyncResult result)
		{
			var documents = AsyncDatabaseCommands.EndMultiGet(result);
			return documents.Select(TrackEntity<T>).ToArray();
		}

		/// <summary>
		/// Begins the async save changes operation
		/// </summary>
		/// <param name="asyncCallback">The async callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		public IAsyncResult BeginSaveChanges(AsyncCallback asyncCallback, object state)
		{
			var data = PrepareForSaveChanges();
			var asyncResult = AsyncDatabaseCommands.BeginBatch(data.Commands.ToArray(), asyncCallback, state);
			return new WrapperAsyncData<DocumentSession.SaveChangesData>(asyncResult, data);
		}

		/// <summary>
		/// Ends the async save changes operation
		/// </summary>
		/// <param name="result">The result.</param>
		public void EndSaveChanges(IAsyncResult result)
		{
			var wrapperAsyncData = ((WrapperAsyncData<DocumentSession.SaveChangesData>)result);

			var batchResults = AsyncDatabaseCommands.EndBatch(wrapperAsyncData.Inner);
			UpdateBatchResults(batchResults, wrapperAsyncData.Wrapped.Entities);
		}

        /// <summary>
        /// Get the json document by key from the store
        /// </summary>
	    protected override JsonDocument GetJsonDocument(string documentKey)
	    {
	        throw new NotSupportedException("Cannot get a document in a syncronous manner using async document session");
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
	}
}
