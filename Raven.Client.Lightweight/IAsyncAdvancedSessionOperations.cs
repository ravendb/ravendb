//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Indexes;
using Raven.Client.Document.Batches;

namespace Raven.Client
{
	/// <summary>
	/// Advanced async session operations
	/// </summary>
	public interface IAsyncAdvancedSessionOperations : IAdvancedDocumentSessionOperations
	{
       
        /// <summary>
        /// Access the eager operations
        /// </summary>
        /// <summary>
        /// Access the lazy operations
        /// </summary>
        IAsyncLazySessionOperations Lazily { get; }

        /// <summary>
        /// Access the eager operations
        /// </summary>
        IAsyncEagerSessionOperations Eagerly { get; }
		/// <summary>
		/// Load documents with the specified key prefix
		/// </summary>
		Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null);

		/// <summary>
		///  Loads documents with the specified key prefix and applies the specified results transformer against the results
		/// </summary>
		/// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
		/// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
		Task<IEnumerable<TResult>> LoadStartingWithAsync<TTransformer, TResult>(string keyPrefix, string matches = null,
		                                                                   int start = 0,
		                                                                   int pageSize = 25, string exclude = null,
		                                                                   RavenPagingInformation pagingInformation = null,
		                                                                   Action<ILoadConfiguration> configure = null)
			where TTransformer : AbstractTransformerCreationTask, new();

	    /// <summary>
	    /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
	    /// </summary>
	    /// <typeparam name="T">The result of the query</typeparam>
	    /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
	    /// <returns></returns>
        [Obsolete("Use AsyncDocumentQuery instead")]
	    IAsyncDocumentQuery<T> AsyncLuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

            /// <summary>
		/// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
        [Obsolete("Use AsyncDocumentQuery instead")]
		IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index, bool isMapReduce = false);

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string index, bool isMapReduce = false);

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
        [Obsolete("Use AsyncDocumentQuery instead")]
		IAsyncDocumentQuery<T> AsyncLuceneQuery<T>();

        /// <summary>
        /// Dynamically query RavenDB using Lucene syntax
        /// </summary>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T>();

		/// <summary>
		/// Stores the specified entity with the specified etag.
		/// The entity will be saved when <see cref="IAsyncDocumentSession.SaveChangesAsync"/> is called.
		/// </summary>
		void Store(object entity, Etag etag);

		/// <summary>
		/// Stores the specified entity in the session. The entity will be saved when <see cref="IAsyncDocumentSession.SaveChangesAsync"/> is called.
		/// </summary>
		/// <param name="entity">The entity.</param>
		void Store(object entity);

		/// <summary>
		/// Stores the specified entity with the specified etag, under the specified id
		/// </summary>
		void Store(object entity, Etag etag, string id);

		/// <summary>
		/// Stores the specified dynamic entity, under the specified id
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <param name="id">The id to store this entity under. If other entity exists with the same id it will be overridden.</param>
		void Store(object entity, string id);


		/// <summary>
		/// Stream the results on the query to the client, converting them to 
		/// CLR types along the way.
		/// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
		/// </summary>
		Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query);

		/// <summary>
		/// Stream the results on the query to the client, converting them to 
		/// CLR types along the way.
		/// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
		/// </summary>
		Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query);

		/// <summary>
		/// Stream the results on the query to the client, converting them to 
		/// CLR types along the way.
		/// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
		/// </summary>
		Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation);

		/// <summary>
		/// Stream the results on the query to the client, converting them to 
		/// CLR types along the way.
		/// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
		/// </summary>
		Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformation);


		/// <summary>
		/// Stream the results of documents searhcto the client, converting them to CLR types along the way.
		/// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
		/// </summary>
		Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(Etag fromEtag, int start = 0, int pageSize = int.MaxValue, RavenPagingInformation pagingInformation = null);


		/// <summary>
		/// Stream the results of documents searhcto the client, converting them to CLR types along the way.
		/// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
		/// </summary>
		Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0, int pageSize = int.MaxValue, RavenPagingInformation pagingInformation = null);
	}
}