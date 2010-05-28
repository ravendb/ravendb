using System;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client.Client
{
	public interface IAsyncDatabaseCommands : IDisposable
	{
		IAsyncResult BeginGet(string key, AsyncCallback callback, object state);
		JsonDocument EndGet(IAsyncResult result);

		IAsyncResult BeginMultiGet(string[] keys, AsyncCallback callback, object state);
		JsonDocument[] EndMultiGet(IAsyncResult result);

		IAsyncResult BeginQuery(string index, IndexQuery query, AsyncCallback callback, object state);
		QueryResult EndQuery(IAsyncResult result);

		IAsyncResult BeginBatch(ICommandData[] commandDatas, AsyncCallback callback, object state);
		BatchResult[] EndBatch(IAsyncResult result);
	}
}