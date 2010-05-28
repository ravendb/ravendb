using System;

namespace Raven.Client
{
	public interface IAsyncDocumentSession : IInMemoryDocumentSessionOperations
	{
		IAsyncResult BeginLoad(string id, AsyncCallback asyncCallback, object state);
		T EndLoad<T>(IAsyncResult result);

		IAsyncResult BeginSaveChanges(AsyncCallback asyncCallback, object state);
		void EndSaveChanges(IAsyncResult result);
	}
}