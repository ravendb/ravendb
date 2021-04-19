//-----------------------------------------------------------------------
// <copyright file="QueryStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Statistics about a raven query.
    /// Such as how many records match the query
    /// </summary>
    public class QueryStatistics
    {
        /// <summary>
        /// Whether the query returned potentially stale results
        /// </summary>
        public bool IsStale { get; set; }

        /// <summary>
        /// The duration of the query _server side_
        /// </summary>
        public long DurationInMs { get; set; }

        /// <summary>
        /// What was the total count of the results that matched the query
        /// </summary>
        public int TotalResults { get; set; }

        /// <summary>
        /// What was the total count of the results that matched the query as int64
        /// </summary>
        public long TotalResults64 { get; set; }

        /// <summary>
        /// Gets or sets the skipped results
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
        /// The timestamp of the last time the index was queried
        /// </summary>
        public DateTime LastQueryTime { get; set; }

        public long? ResultEtag { get; set; }

        /// <summary>
        /// Tag of a cluster node which responded to the query
        /// </summary>
        public string NodeTag { get; set; }

        /// <summary>
        /// Update the query stats from the query results
        /// </summary>
        internal void UpdateQueryStats(QueryResult qr)
        {
            IsStale = qr.IsStale;
            DurationInMs = qr.DurationInMs;
            TotalResults = qr.TotalResults;
            TotalResults64 = qr.TotalResults64;
            SkippedResults = qr.SkippedResults;
            Timestamp = qr.IndexTimestamp;
            IndexName = qr.IndexName;
            IndexTimestamp = qr.IndexTimestamp;
            LastQueryTime = qr.LastQueryTime;
            ResultEtag = qr.ResultEtag;
            NodeTag = qr.NodeTag;
        }
    }
}
