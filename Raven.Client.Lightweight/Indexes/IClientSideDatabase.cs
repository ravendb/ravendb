//-----------------------------------------------------------------------
// <copyright file="IClientSideDatabase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// DatabaseAccessor for loading documents in the translator
	/// </summary>
	public interface IClientSideDatabase
	{
		/// <summary>
		/// Loading a document during result transformers
		/// </summary>
		T Load<T>(string docId);

		/// <summary>
		/// Loading documents during result transformers
		/// </summary>
		T[] Load<T>(IEnumerable<string> docIds);

		/// <summary>
		/// Will ask RavenDB to include this document in the query results
		/// </summary>
		object Include(string docId);

		/// <summary>
		/// Will ask RavenDB to include these documents in the query results
		/// </summary>
		object Include(IEnumerable<string> docId);
	}
}