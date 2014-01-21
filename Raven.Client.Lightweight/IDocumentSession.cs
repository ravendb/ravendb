//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Client
{
	/// <summary>
	/// Interface for document session
	/// </summary>
	public interface IDocumentSession : IDisposable
	{
		/// <summary>
		/// Get the accessor for advanced operations
		/// </summary>
		/// <remarks>
		/// Those operations are rarely needed, and have been moved to a separate 
		/// property to avoid cluttering the API
		/// </remarks>
		ISyncAdvancedSessionOperation Advanced { get; }

		/// <summary>
		/// Marks the specified entity for deletion. The entity will be deleted when <see cref="IDocumentSession.SaveChanges"/> is called.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="entity">The entity.</param>
		void Delete<T>(T entity);

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// WARNING: This method will not call beforeDelete listener!
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The entity.</param>
	    void Delete<T>(ValueType id);

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// WARNING: This method will not call beforeDelete listener!
        /// </summary>
        /// <param name="id"></param>
	    void Delete(string id);

		/// <summary>
		/// Loads the specified entity with the specified id.
		/// </summary>
		/// <param name="id">The id.</param>
		T Load<T>(string id);

		/// <summary>
		/// Loads the specified entities with the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		T[] Load<T>(params string[] ids);

		/// <summary>
		/// Loads the specified entities with the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		T[] Load<T>(IEnumerable<string> ids);

		/// <summary>
		/// Loads the specified entity with the specified id after applying
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
		T Load<T>(ValueType id);

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1,2,3)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		T[] Load<T>(params ValueType[] ids);

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(new List&lt;int&gt;(){1,2,3})
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		T[] Load<T>(IEnumerable<ValueType> ids);

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

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILoaderWithInclude<object> Include(string path);

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path);

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path);

	    /// <summary>
	    /// Performs a load that will use the specified results transformer against the specified id
	    /// </summary>
	    /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
	    /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
	    /// <returns></returns>
	    TResult Load<TTransformer, TResult>(string id) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        /// Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id"></param>
        /// <param name="configure"></param>
        /// <returns></returns>
        TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new();

		/// <summary>
		/// Performs a load that will use the specified results transformer against the specified id
		/// </summary>
		/// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
		/// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
		/// <returns></returns>
		TResult[] Load<TTransformer, TResult>(params string[] ids) where TTransformer : AbstractTransformerCreationTask, new();

		/// <summary>
		/// Performs a load that will use the specified results transformer against the specified id
		/// </summary>
		TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new();

		/// <summary>
		/// Saves all the changes to the Raven server.
		/// </summary>
		void SaveChanges();

		/// <summary>
		/// Stores the specified entity with the specified etag
		/// </summary>
		void Store(object entity, Etag etag);

		/// <summary>
		/// Stores the specified entity with the specified etag, under the specified id
		/// </summary>
		void Store(object entity, Etag etag, string id);

		/// <summary>
		/// Stores the specified dynamic entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		void Store(dynamic entity);

		/// <summary>
		/// Stores the specified dynamic entity, under the specified id
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <param name="id">The id to store this entity under. If other entity exists with the same id it will be overridden.</param>
		void Store(dynamic entity, string id);


	}
}

#endif
