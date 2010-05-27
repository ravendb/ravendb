using System;

namespace Raven.Client
{
	public interface IAsyncDocumentSession : IDisposable
	{
		IAsyncResult BeginLoad(string id, AsyncCallback asyncCallback, object state);
		T EndLoad<T>(IAsyncResult result);
	}
}