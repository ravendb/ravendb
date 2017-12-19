//-----------------------------------------------------------------------
// <copyright file="QueryResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// The result of a query
    /// </summary>
    public abstract class QueryResult<TResult, TIncludes> : QueryResultBase<TResult, TIncludes>
    {
        /// <summary>
        /// Gets or sets the total results for this query
        /// </summary>
        public int TotalResults { get; set; }

        /// <summary>
        /// Gets or sets the skipped results
        /// </summary>
        public int SkippedResults { get; set; }

#if FEATURE_HIGHLIGHTING
        /// <summary>
        /// Highlighter results (if requested).
        /// </summary>
        public Dictionary<string, Dictionary<string, string[]>> Highlightings { get; set; }
#endif

        /// <summary>
        /// The duration of actually executing the query server side
        /// </summary>
        public long DurationInMs { get; set; }

        /// <summary>
        /// Explanations of document scores (if requested).
        /// </summary>
        public Dictionary<string, string> ScoreExplanations { get; set; }

        /// <summary>
        /// Detailed timings for various parts of a query (Lucene search, loading documents, transforming results) - if requested.
        /// </summary>
        public Dictionary<string, double> TimingsInMs { get; set; }

        /// <summary>
        /// The size of the request which were sent from the server.
        /// This value is the _uncompressed_ size. 
        /// </summary>
        public long ResultSize { get; set; }
    }

    public class QueryResult : QueryResult<BlittableJsonReaderArray, BlittableJsonReaderObject>
    {
        /// <summary>
        /// Creates a snapshot of the query results
        /// </summary>
        public QueryResult CreateSnapshot()
        {
#if DEBUG
            Results.BlittableValidation();
#endif
            return new QueryResult
            {
                Results = Results,
                Includes = Includes,
                IndexName = IndexName,
                IndexTimestamp = IndexTimestamp,
                IncludedPaths = IncludedPaths,
                IsStale = IsStale,
                SkippedResults = SkippedResults,
                TotalResults = TotalResults,
#if FEATURE_HIGHLIGHTING
                Highlightings = Highlightings?.ToDictionary(pair => pair.Key, x => new Dictionary<string, string[]>(x.Value)),
#endif
                ScoreExplanations = ScoreExplanations?.ToDictionary(x => x.Key, x => x.Value),
                TimingsInMs = TimingsInMs?.ToDictionary(x => x.Key, x => x.Value),
                LastQueryTime = LastQueryTime,
                DurationInMs = DurationInMs,
                ResultEtag = ResultEtag
            };
        }
    }
}
