namespace Raven.Client.Documents.Indexes
{
    public sealed class PutIndexResult
    {
        /// <summary>
        /// Gets or sets the total results for this query
        /// </summary>
        public string Index { get; set; }

        public long RaftCommandIndex { get; set; }
    }
}