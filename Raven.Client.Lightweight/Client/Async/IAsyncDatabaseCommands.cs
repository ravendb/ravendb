using System;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client.Client.Async
{
	/// <summary>
	/// An async database command operations
	/// </summary>
	public interface IAsyncDatabaseCommands : IDisposable
	{
		/// <summary>
		/// Begins an async get operation
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		IAsyncResult BeginGet(string key, AsyncCallback callback, object state);
		/// <summary>
		/// Ends the async get operation
		/// </summary>
		/// <param name="result">The result.</param>
		JsonDocument EndGet(IAsyncResult result);

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		/// <param name="keys">The keys.</param>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		IAsyncResult BeginMultiGet(string[] keys, AsyncCallback callback, object state);
		/// <summary>
		/// Ends the async multi get operation
		/// </summary>
		/// <param name="result">The result.</param>
		JsonDocument[] EndMultiGet(IAsyncResult result);

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		IAsyncResult BeginQuery(string index, IndexQuery query, AsyncCallback callback, object state);
		/// <summary>
		/// Ends the async query.
		/// </summary>
		/// <param name="result">The result.</param>
		/// <returns></returns>
		QueryResult EndQuery(IAsyncResult result);

		/// <summary>
		/// Begins the async batch operation
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		IAsyncResult BeginBatch(ICommandData[] commandDatas, AsyncCallback callback, object state);
		/// <summary>
		/// Ends the async batch operation
		/// </summary>
		/// <param name="result">The result.</param>
		BatchResult[] EndBatch(IAsyncResult result);
	}
}
