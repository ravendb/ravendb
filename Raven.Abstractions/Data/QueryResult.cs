//-----------------------------------------------------------------------
// <copyright file="QueryResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Database.Data
{
	/// <summary>
	/// The result of a query
	/// </summary>
	public class QueryResult
	{
		/// <summary>
		/// Gets or sets the document resulting from this query.
		/// </summary>
		/// <value>The results.</value>
		public List<RavenJObject> Results { get; set; }
		/// <summary>
		/// Gets or sets the document included in the result.
		/// </summary>
		/// <value>The includes.</value>
		public List<RavenJObject> Includes { get; set; }
		/// <summary>
		/// Gets or sets a value indicating whether the index is stale.
		/// </summary>
		/// <value><c>true</c> if the index is stale; otherwise, <c>false</c>.</value>
		public bool IsStale { get; set; }

        /// <summary>
        /// The last time the index was updated.
        /// This can be used to determine the freshness of the data.
        /// </summary>
        public DateTime IndexTimestamp { get; set; }

		/// <summary>
		/// Gets or sets the total results for this query
		/// </summary>
		/// <value>The total results.</value>
		public int TotalResults { get; set; }
		/// <summary>
		/// Gets or sets the skipped results (duplicate documents);
		/// </summary>
		/// <value>The skipped results.</value>
        public int SkippedResults { get; set; }

		/// <summary>
		/// The index used to answer this query
		/// </summary>
		public string IndexName { get; set; }

        /// <summary>
        /// The last etag indexed by the index.
        /// This can be used to determine whatever the results can be cached.
        /// </summary>
        public Guid IndexEtag { get; set; }

	    /// <summary>
		/// Initializes a new instance of the <see cref="QueryResult"/> class.
		/// </summary>
		public QueryResult()
		{
			Results = new List<RavenJObject>();
			Includes = new List<RavenJObject>();
		}
	}
}
