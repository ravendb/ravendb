//-----------------------------------------------------------------------
// <copyright file="RavenQueryStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;

namespace Raven.NewClient.Client
{
    /// <summary>
    /// Statistics about a raven query.
    /// Such as how many records match the query
    /// </summary>
    public class RavenQueryStatistics
    {
        public RavenQueryStatistics()
        {
            TimingsInMilliseconds = new Dictionary<string, double>();
        }

        /// <summary>
        /// Whatever the query returned potentially stale results
        /// </summary>
        public bool IsStale { get; set; }

        /// <summary>
        /// The duration of the query _server side_
        /// </summary>
        public long DurationMilliseconds { get; set; }

        /// <summary>
        /// What was the total count of the results that matched the query
        /// </summary>
        public int TotalResults { get; set; }

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

        /// <summary>
        /// Detailed timings for various parts of a query (Lucene search, loading documents, transforming results)
        /// </summary>
        public Dictionary<string, double> TimingsInMilliseconds { get; set; }



        public long? ResultEtag { get; set; }

        /// <summary>
        /// The size of the request which were sent from the server.
        /// This value is the _uncompressed_ size. 
        /// </summary>
        public long ResultSize { get; set; }

        /// <summary>
        /// Update the query stats from the query results
        /// </summary>
        internal void UpdateQueryStats(QueryResult qr)
        {
            IsStale = qr.IsStale;
            DurationMilliseconds = qr.DurationMilliseconds;
            TotalResults = qr.TotalResults;
            SkippedResults = qr.SkippedResults;
            Timestamp = qr.IndexTimestamp;
            IndexName = qr.IndexName;
            IndexTimestamp = qr.IndexTimestamp;
            TimingsInMilliseconds = qr.TimingsInMilliseconds;
            LastQueryTime = qr.LastQueryTime;
            ResultSize = qr.ResultSize;
            ResultEtag = qr.ResultEtag;
            ScoreExplanations = qr.ScoreExplanations;
        }

        /// <summary>
        /// Gets or sets explanations of document scores 
        /// </summary>
        public Dictionary<string, string> ScoreExplanations { get; set; }
    }
}
