//-----------------------------------------------------------------------
// <copyright file="IDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
#if !NET_3_5
using System.Threading.Tasks;
#endif
using Raven.Abstractions.Data;
using Raven.Client.Document;

namespace Raven.Client
{
	/// <summary>
	/// A query against a Raven index
	/// </summary>
	public interface IDocumentQuery<T> : IEnumerable<T>, IDocumentQueryBase<T, IDocumentQuery<T>>
	{

        /// <summary>
        /// Selects the specified fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <param name="fields">The fields.</param>
        IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

        /// <summary>
        /// Gets the document convention from the query session
        /// </summary>
	    DocumentConvention DocumentConvention { get; }

#if !SILVERLIGHT
		/// <summary>
		/// Gets the query result
		/// Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		QueryResult QueryResult { get; }
#endif

#if !NET_3_5
		/// <summary>
		/// Gets the query result
		/// </summary>
		/// <value>The query result.</value>
		Task<IList<T>> ToListAsync();

		/// <summary>
		/// Gets the total count of records for this query
		/// </summary>
		Task<int> CountAsync();
#endif
	}
}
