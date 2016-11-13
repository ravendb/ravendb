using Raven.Abstractions.FileSystem;

namespace Raven.NewClient.Client.FileSystem
{
    /// <summary>
    /// Statistics about a the files query.
    /// Such as how many records match the query
    /// </summary>
    public class FilesQueryStatistics
    {
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
        /// Update the query stats from the query results
        /// </summary>
        internal void UpdateQueryStats(SearchResults searchResults)
        {
            DurationMilliseconds = searchResults.DurationMilliseconds;
            TotalResults = searchResults.FileCount;
        }
    }
}
