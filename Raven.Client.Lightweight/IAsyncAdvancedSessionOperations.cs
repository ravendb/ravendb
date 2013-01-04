//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
		IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index);

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		IAsyncDocumentQuery<T> AsyncLuceneQuery<T>();
	}
}