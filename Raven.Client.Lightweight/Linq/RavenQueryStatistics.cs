using System;

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
    }
}