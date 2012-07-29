//-----------------------------------------------------------------------
// <copyright file="RavenQueryStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel;
using Raven.Abstractions.Data;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Statistics about a raven query.
	/// Such as how many records match the query
	/// </summary>
	public class RavenQueryStatistics
	{
		/// <summary>
		/// Whatever the query returned potentially stale results
		/// </summary>
		public bool IsStale { get; set; }

		/// <summary>
		/// What was the total count of the results that matched the query
		/// </summary>
		public int TotalResults { get; set; }

		/// <summary>
		/// Gets or sets the skipped results (duplicate documents);
		/// </summary>
		public int SkippedResults { get; set; }

		/// <summary>
		/// The time when the query results were unstale.
		/// </summary>
		public DateTime Timestamp { get; set; }

		/// <summary>
		/// The name of the index queried
		/// </summary>
		public string IndexName { get; set; }

		/// <summary>
		/// The timestamp of the queried index
		/// </summary>
		public DateTime IndexTimestamp { get; set; }

		/// <summary>
		/// The etag of the queried index
		/// </summary>
		public Guid IndexEtag { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether any of the documents returned by this query
		/// are non authoritative (modified by uncommitted transaction).
		/// </summary>
		public bool NonAuthoritativeInformation { get; set; }

		/// <summary>
		/// The timestamp of the last time the index was queried
		/// </summary>
		public bool LastQueryTime { get; set; }

		/// <summary>
		/// Update the query stats from the query results
		/// </summary>
		internal void UpdateQueryStats(QueryResult qr)
		{
			IsStale = qr.IsStale;
			NonAuthoritativeInformation= qr.NonAuthoritativeInformation;
			TotalResults = qr.TotalResults;
			SkippedResults = qr.SkippedResults;
			Timestamp = qr.IndexTimestamp;
			IndexName = qr.IndexName;
			IndexTimestamp = qr.IndexTimestamp;
			IndexEtag = qr.IndexEtag;
			//LastQueryTime = qr.LastQueryTime;
		}

		
	}
}
