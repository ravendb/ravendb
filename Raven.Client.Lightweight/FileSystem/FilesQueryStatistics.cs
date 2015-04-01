namespace Raven.Client.FileSystem
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
        /// Gets or sets a value indicating whether any of the documents returned by this query
        /// are non authoritative (modified by uncommitted transaction).
        /// </summary>
        public bool NonAuthoritativeInformation { get; set; }
    }
}
