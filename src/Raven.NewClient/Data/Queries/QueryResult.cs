//-----------------------------------------------------------------------
// <copyright file="QueryResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

using Raven.NewClient.Json.Linq;
using Sparrow.Json;

namespace Raven.NewClient.Client.Data.Queries
{
    /// <summary>
    /// The result of a query
    /// </summary>
    public class QueryResult : QueryResultBase
    {
        /// <summary>
        /// Gets or sets the total results for this query
        /// </summary>
        public int TotalResults { get; set; }

        /// <summary>
        /// Gets or sets the skipped results
        /// </summary>
        public int SkippedResults { get; set; }

        /// <summary>
        /// Highlighter results (if requested).
        /// </summary>
        public Dictionary<string, Dictionary<string, string[]>> Highlightings { get; set; }

        /// <summary>
        /// The duration of actually executing the query server side
        /// </summary>
        public long DurationMilliseconds { get; set; }

        /// <summary>
        /// Explanations of document scores (if requested).
        /// </summary>
        public Dictionary<string, string> ScoreExplanations { get; set; }

        /// <summary>
        /// Detailed timings for various parts of a query (Lucene search, loading documents, transforming results) - if requested.
        /// </summary>
        public Dictionary<string, double> TimingsInMilliseconds { get; set; }

        /// <summary>
        /// The size of the request which were sent from the server.
        /// This value is the _uncompressed_ size. 
        /// </summary>
        public long ResultSize { get; set; }

        /// <summary>
        /// Ensures that the query results can be used in snapshots
        /// </summary>
        public void EnsureSnapshot()
        {
            foreach (BlittableJsonReaderObject result in Results.Items)
            {
                //result.EnsureCannotBeChangeAndEnableSnapshotting();
            }
            foreach (BlittableJsonReaderObject result in Includes)
            {
                //result.EnsureCannotBeChangeAndEnableSnapshotting();
            }
        }

        /// <summary>
        /// Creates a snapshot of the query results
        /// </summary>
        public QueryResult CreateSnapshot()
        {
            return new QueryResult
            {
                Results = Results,
                Includes = Includes,
                IndexName = IndexName,
                IndexTimestamp = IndexTimestamp,
                IsStale = IsStale,
                SkippedResults = SkippedResults,
                TotalResults = TotalResults,
                Highlightings = Highlightings?.ToDictionary(
                    pair => pair.Key,
                    x => new Dictionary<string, string[]>(x.Value)),
                ScoreExplanations = ScoreExplanations?.ToDictionary(x => x.Key, x => x.Value),
                TimingsInMilliseconds = TimingsInMilliseconds?.ToDictionary(x => x.Key, x => x.Value),
                LastQueryTime = LastQueryTime,
                DurationMilliseconds = DurationMilliseconds,
                ResultEtag = ResultEtag
            };
        }
    }
}
