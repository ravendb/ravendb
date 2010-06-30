using System;
using System.Linq;
using System.Net;
using Raven.Client.Client;
using Raven.Client.Client.Async;
using Raven.Database;

namespace Raven.Client.Document.Async
{
	public class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSession
	{
		public AsyncDocumentSession(DocumentStore documentStore, IDocumentStoreListener[] storeListeners, IDocumentDeleteListener[] deleteListeners)
			: base(documentStore, storeListeners, deleteListeners)
		{
		}

		private IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return documentStore.AsyncDatabaseCommands; }
		}

		public IAsyncResult BeginLoad(string id, AsyncCallback asyncCallback, object state)
		{
			object entity;
			if (entitiesByKey.TryGetValue(id, out entity))
				return new SyncronousLoadResult(state, entity);
			
			IncrementRequestCount();

			return AsyncDatabaseCommands.BeginGet(id, asyncCallback, state);
		}

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

		public IAsyncResult BeginMultiLoad(string[] ids, AsyncCallback asyncCallback, object state)
		{
			IncrementRequestCount();
			return AsyncDatabaseCommands.BeginMultiGet(ids, asyncCallback, state);
		}

		public T[] EndMultiLoad<T>(IAsyncResult result)
		{
			var documents = AsyncDatabaseCommands.EndMultiGet(result);
			return documents.Select(TrackEntity<T>).ToArray();
		}

		public IAsyncResult BeginSaveChanges(AsyncCallback asyncCallback, object state)
		{
			var data = PrepareForSaveChanges();
			var asyncResult = AsyncDatabaseCommands.BeginBatch(data.Commands.ToArray(), asyncCallback, state);
			return new WrapperAsyncData<DocumentSession.SaveChangesData>(asyncResult, data);
		}

		public void EndSaveChanges(IAsyncResult result)
		{
			var wrapperAsyncData = ((WrapperAsyncData<DocumentSession.SaveChangesData>)result);

			var batchResults = AsyncDatabaseCommands.EndBatch(wrapperAsyncData.Inner);
			UpdateBatchResults(batchResults, wrapperAsyncData.Wrapped.Entities);
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
	}
}