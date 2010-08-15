using System;
using Raven.Client.Client.Async;

namespace Raven.Client
{
	public interface IAsyncDocumentSession : IInMemoryDocumentSessionOperations
	{
		IAsyncDatabaseCommands AsyncDatabaseCommands { get; }
		
		IAsyncResult BeginLoad(string id, AsyncCallback asyncCallback, object state);
		T EndLoad<T>(IAsyncResult result);

		IAsyncResult BeginMultiLoad(string[] ids, AsyncCallback asyncCallback, object state);
		T[] EndMultiLoad<T>(IAsyncResult result);

		IAsyncResult BeginSaveChanges(AsyncCallback asyncCallback, object state);
		void EndSaveChanges(IAsyncResult result);
	}
}