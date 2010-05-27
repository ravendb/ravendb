using System;
using System.Net;
using System.Threading;
using Raven.Client.Client;
using Raven.Database;

namespace Raven.Client.Document
{
	public class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSession
	{
		public AsyncDocumentSession(DocumentStore documentStore) : base(documentStore)
		{
		}

		private IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return documentStore.AsyncDatabaseCommands; }
		}

		public void Dispose()
		{
			
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


		public class SyncronousLoadResult : IAsyncResult
		{
			private readonly object state;
			private readonly object entity;

			public object Entity
			{
				get { return entity; }
			}

			public bool IsCompleted
			{
				get { return true; }
			}

			public WaitHandle AsyncWaitHandle
			{
				get { return null; }
			}

			public object AsyncState
			{
				get { return state; }
			}

			public bool CompletedSynchronously
			{
				get { return true; }
			}

			public SyncronousLoadResult(object state, object entity)
			{
				this.state = state;
				this.entity = entity;
			}
		}

		public event EntityStored Stored;
	}
}