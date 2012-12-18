//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Indexes;

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
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		IAsyncLoaderWithInclude<object> Include(string path);

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path);

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path);

		/// <summary>
		/// Stores the specified entity with the specified etag.
		/// The entity will be saved when <see cref="SaveChangesAsync"/> is called.
		/// </summary>
		void Store(object entity, Guid etag);

		/// <summary>
		/// Stores the specified entity in the session. The entity will be saved when <see cref="SaveChangesAsync"/> is called.
		/// </summary>
		/// <param name="entity">The entity.</param>
		void Store(object entity);

		/// <summary>
		/// Stores the specified entity with the specified etag, under the specified id
		/// </summary>
		void Store(object entity, Guid etag, string id);

		/// <summary>
		/// Stores the specified dynamic entity, under the specified id
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <param name="id">The id to store this entity under. If other entity exists with the same id it will be overridden.</param>
		void Store(object entity, string id);

		/// <summary>
		/// Marks the specified entity for deletion. The entity will be deleted when <see cref="SaveChangesAsync"/> is called.
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
		Task<T[]> LoadAsync<T>(string[] ids);

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

		/// <summary>
		/// Queries the index specified by <typeparamref name="TIndexCreator"/> using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();
	}
}
