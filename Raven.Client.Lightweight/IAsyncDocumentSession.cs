//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5

using System;
using System.Threading.Tasks;

namespace Raven.Client
{
	using Linq;

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
		/// <returns></returns>
		Task<T> LoadAsync<T>(string id);

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<T> LoadAsync<T>(ValueType id);

		/// <summary>
		/// Begins the async multi load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Task<T[]> MultiLoadAsync<T>(string[] ids);
		
		/// <summary>
		/// Begins the async save changes operation
		/// </summary>
		/// <returns></returns>
		Task SaveChangesAsync();

		/// <summary>
		/// Queries the specified index using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <param name="indexName">Name of the index.</param>
		IRavenQueryable<T> Query<T>(string indexName);

		/// <summary>
		/// Dynamically queries RavenDB using LINQ
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		IRavenQueryable<T> Query<T>();
	}
}
#endif