//-----------------------------------------------------------------------
// <copyright file="QueryResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Abstractions.Data
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
		/// Gets or sets the skipped results
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
		public Etag IndexEtag { get; set; }

		/// <summary>
		/// The ETag value for this index current state, which include what we docs we indexed,
		/// what document were deleted, etc.
		/// </summary>
		public Etag ResultEtag { get; set; }

        /// <summary>
        /// Gets or sets highlighter results 
        /// </summary>
        public Dictionary<string, Dictionary<string, string[]>> Highlightings { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether any of the documents returned by this query
		/// are non authoritative (modified by uncommitted transaction).
		/// </summary>
		public bool NonAuthoritativeInformation { get; set; }

		/// <summary>
		/// The timestamp of the last time the index was queried
		/// </summary>
		public DateTime LastQueryTime { get; set; }

		/// <summary>
		/// The duration of actually executing the query server side
		/// </summary>
		public long DurationMilliseconds { get; set; }

		/// <summary>
		/// Gets or sets explanations of document scores 
		/// </summary>
		public Dictionary<string, string> ScoreExplanations { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="QueryResult"/> class.
		/// </summary>
		public QueryResult()
		{
			Results = new List<RavenJObject>();
			Includes = new List<RavenJObject>();
		    Highlightings = new Dictionary<string, Dictionary<string, string[]>>();
		}

		/// <summary>
		/// Ensures that the query results can be used in snapshots
		/// </summary>
		public void EnsureSnapshot()
		{
			foreach (var result in Results)
			{
				result.EnsureCannotBeChangeAndEnableSnapshotting();
			}
			foreach (var result in Includes)
			{
				result.EnsureCannotBeChangeAndEnableSnapshotting();
			}
		}

		/// <summary>
		/// Creates a snapshot of the query results
		/// </summary>
		public QueryResult CreateSnapshot()
		{
			return new QueryResult
			{
				Results = new List<RavenJObject>(this.Results.Select(x => (RavenJObject)x.CreateSnapshot())),
				Includes = new List<RavenJObject>(this.Includes.Select(x => (RavenJObject)x.CreateSnapshot())),
				IndexEtag = this.IndexEtag,
				IndexName = this.IndexName,
				IndexTimestamp = this.IndexTimestamp,
				IsStale = this.IsStale,
				SkippedResults = this.SkippedResults,
				TotalResults = this.TotalResults,
				Highlightings = this.Highlightings.ToDictionary(
					pair => pair.Key,
					x => new Dictionary<string, string[]>(x.Value)),
				ScoreExplanations = this.ScoreExplanations.ToDictionary(x => x.Key, x => x.Value)
			};
		}
	}
}
