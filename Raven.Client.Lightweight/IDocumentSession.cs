//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Client
{
#if !SILVERLIGHT
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

		
		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILoaderWithInclude<object> Include(string path);

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILoaderWithInclude<T> Include<T>(Expression<Func<T,object>> path);

	
		/// <summary>
		/// Saves all the changes to the Raven server.
		/// </summary>
		void SaveChanges();

#if !NET_3_5        
		/// <summary>
		/// Stores the specified dynamic entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		void Store(dynamic entity);
#else 
		/// <summary>
		/// Stores the specified dynamic entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		void Store(object entity);
#endif
	}
#endif
}
