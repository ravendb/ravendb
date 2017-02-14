using Raven.Client.Documents.Indexes;

namespace Indexing.Benchmark
{
    public class IndexingTestRun
    {
        public int NumberOfRelevantDocs { get; set; }

        public AbstractIndexCreationTask Index { get; set; }
    }
}