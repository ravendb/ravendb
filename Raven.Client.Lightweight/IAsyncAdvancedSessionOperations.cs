//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Client
{
	/// <summary>
	/// Advanced async session operations
	/// </summary>
	public interface IAsyncAdvancedSessionOperations : IAdvancedDocumentSessionOperations
	{
		/// <summary>
		/// Load documents with the specified key prefix
		/// </summary>
		Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, int start = 0, int pageSize = 25);


		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index, bool isMapReduce = false);

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		IAsyncDocumentQuery<T> AsyncLuceneQuery<T>();

		/// <summary>
		/// Stores the specified entity with the specified etag.
		/// The entity will be saved when <see cref="IAsyncDocumentSession.SaveChangesAsync"/> is called.
		/// </summary>
		void Store(object entity, Guid etag);

		/// <summary>
		/// Stores the specified entity in the session. The entity will be saved when <see cref="IAsyncDocumentSession.SaveChangesAsync"/> is called.
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
	}
}