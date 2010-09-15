using System;
using Raven.Client.Client.Async;

namespace Raven.Client
{
	/// <summary>
	/// Interface for document session using async approaches
	/// </summary>
	public interface IAsyncDocumentSession : IInMemoryDocumentSessionOperations
	{
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		IAsyncDatabaseCommands AsyncDatabaseCommands { get; }

		/// <summary>
		/// Begins the aysnc load operation
		/// </summary>
		/// <param name="id">The id.</param>
		/// <param name="asyncCallback">The async callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		IAsyncResult BeginLoad(string id, AsyncCallback asyncCallback, object state);
		/// <summary>
		/// Ends the async load operation
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="result">The result.</param>
		/// <returns></returns>
		T EndLoad<T>(IAsyncResult result);

		/// <summary>
		/// Begins the async multi load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <param name="asyncCallback">The async callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		IAsyncResult BeginMultiLoad(string[] ids, AsyncCallback asyncCallback, object state);
		/// <summary>
		/// Ends the async multi load operation
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="result">The result.</param>
		/// <returns></returns>
		T[] EndMultiLoad<T>(IAsyncResult result);

		/// <summary>
		/// Begins the async save changes operation
		/// </summary>
		/// <param name="asyncCallback">The async callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		IAsyncResult BeginSaveChanges(AsyncCallback asyncCallback, object state);
		/// <summary>
		/// Ends the async save changes operation
		/// </summary>
		/// <param name="result">The result.</param>
		void EndSaveChanges(IAsyncResult result);
	}
}