//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;

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
		Task StoreAsync(object entity, Etag etag);

		/// <summary>
		/// Stores the specified entity in the session. The entity will be saved when <see cref="SaveChangesAsync"/> is called.
		/// </summary>
		/// <param name="entity">The entity.</param>
		Task StoreAsync(object entity);

		/// <summary>
		/// Stores the specified entity with the specified etag, under the specified id
		/// </summary>
		Task StoreAsync(object entity, Etag etag, string id);
		
		/// <summary>
		/// Stores the specified dynamic entity, under the specified id
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <param name="id">The id to store this entity under. If other entity exists with the same id it will be overridden.</param>
		Task StoreAsync(object entity, string id);

		/// <summary>
		/// Marks the specified entity for deletion. The entity will be deleted when <see cref="SaveChangesAsync"/> is called.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		void Delete<T>(T entity);

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when <see cref="SaveChangesAsync"/> is called.
        /// WARNING: This method will not call beforeDelete listener!
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The entity.</param>
        void Delete<T>(ValueType id);

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when <see cref="SaveChangesAsync"/> is called.
        /// WARNING: This method will not call beforeDelete listener!
        /// </summary>
        /// <param name="id"></param>
        void Delete(string id);

		/// <summary>
		/// Begins the async load operation
		/// </summary>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Task<T> LoadAsync<T>(string id);

		/// <summary>
		/// Begins the async multi-load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Task<T[]> LoadAsync<T>(IEnumerable<string> ids);

		/// <summary>
		/// Begins the async load operation, with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(1)
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<T> LoadAsync<T>(ValueType id);

		/// <summary>
		/// Begins the async multi-load operation, with the specified ids after applying
		/// conventions on the provided ids to get the real document ids.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(1,2,3)
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<T[]> LoadAsync<T>(params ValueType[] ids);

		/// <summary>
		/// Begins the async multi-load operation, with the specified ids after applying
		/// conventions on the provided ids to get the real document ids.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<T[]> LoadAsync<T>(IEnumerable<ValueType> ids);

		/// <summary>
		/// Performs a load that will use the specified results transformer against the specified id
		/// </summary>
		/// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
		/// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
		/// <param name="id"></param>
		/// <param name="configure"></param>
		/// <returns></returns>
		Task<TResult> LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new();

		/// <summary>
		/// Performs a load that will use the specified results transformer against the specified id
		/// </summary>
		Task<TResult[]> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new();

		/// <summary>
		/// Performs a load that will use the specified results transformer against the specified id
		/// </summary>
		/// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
		/// <param name="id"></param>
		/// <param name="transformer">The transformer to use in this load operation</param>
		/// <param name="configure"></param>
		/// <returns></returns>
		Task<TResult> LoadAsync<TResult>(string id, string transformer, Action<ILoadConfiguration> configure = null);

		/// <summary>
		/// Performs a load that will use the specified results transformer against the specified ids
		/// </summary>
		/// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
		/// <param name="ids"></param>
		/// <param name="transformer">The transformer to use in this load operation</param>
		/// <param name="configure"></param>
		/// <returns></returns>
		Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null);

		/// <summary>
		/// Performs a load that will use the specified results transformer against the specified id
		/// </summary>
		/// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
		/// <param name="id"></param>
		/// <param name="transformerType">The transformer to use in this load operation</param>
		/// <param name="configure"></param>
		/// <returns></returns>
		Task<TResult> LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null);

		/// <summary>
		/// Performs a load that will use the specified results transformer against the specified ids
		/// </summary>
		/// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
		/// <param name="ids"></param>
		/// <param name="transformerType">The transformer to use in this load operation</param>
		/// <param name="configure"></param>
		/// <returns></returns>
		Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null);

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
		/// <param name="isMapReduce">Whatever we are querying a map/reduce index (modify how we treat identifier properties)</param>
		IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false);

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
