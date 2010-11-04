using System;
using Raven.Client.Client.Async;

namespace Raven.Client
{
	/// <summary>
	/// Interface for document session using async approaches
	/// </summary>
	public interface IAsyncDocumentSession : IDisposable
	{
        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate 
        /// property to avoid cluttering the API
        /// </remarks>
        IAsyncAdvancedSessionOperations Advanced { get; }

        /// <summary>
        /// Stores the specified entity in the session. The entity will be saved when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// </summary>
        /// <param name="entity">The entity.</param>
        void Store(object entity);

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        void Delete<T>(T entity);

		/// <summary>
		/// Begins the async load operation
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
