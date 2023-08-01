using System;

namespace Raven.Client.Documents.Indexes
{
    public sealed class IndexingPerformanceOperation
    {
        public IndexingPerformanceOperation()
        {
            // for deserialization
        }

        public IndexingPerformanceOperation(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
            Operations = Array.Empty<IndexingPerformanceOperation>();
        }

        public string Name { get; set; }

        public double DurationInMs { get; }

        public ReduceRunDetails ReduceDetails { get; set; }

        public MapRunDetails MapDetails { get; set; }
        
        public CleanupRunDetails CleanupDetails { get; set; }

        public LuceneMergeDetails LuceneMergeDetails { get; set; }

        public StorageCommitDetails CommitDetails { get; set; } 

        public IndexingPerformanceOperation[] Operations { get; set; }

        public ReferenceRunDetails ReferenceDetails { get; set; }
    }
}
