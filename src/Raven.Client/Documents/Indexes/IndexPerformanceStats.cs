namespace Raven.Client.Documents.Indexes
{
    public class IndexPerformanceStats
    {
        public string Name { get; set; }

        public IndexingPerformanceStats[] Performance { get; set; }
    }
}
