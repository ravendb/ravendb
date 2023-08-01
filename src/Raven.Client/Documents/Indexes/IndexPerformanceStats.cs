namespace Raven.Client.Documents.Indexes
{
    public sealed class IndexPerformanceStats
    {
        public string Name { get; set; }

        public IndexingPerformanceStats[] Performance { get; set; }
    }
}
