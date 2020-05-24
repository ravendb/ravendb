using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes
{
    public class IndexProgress
    {
        public string Name { get; set; }

        public IndexType Type { get; set; }

        public IndexSourceType SourceType { get; set; }

        public Dictionary<string, CollectionStats> Collections { get; set; }

        public bool IsStale { get; set; }

        public IndexRunningStatus IndexRunningStatus { get; set; }

        public double ProcessedPerSecond { get; set; }

        public class CollectionStats
        {
            public long LastProcessedDocumentEtag { get; set; }

            public long NumberOfDocumentsToProcess { get; set; }

            public long TotalNumberOfDocuments { get; set; }

            public long LastProcessedTombstoneEtag { get; set; }

            public long NumberOfTombstonesToProcess { get; set; }

            public long TotalNumberOfTombstones { get; set; }

            internal void UpdateLastEtag(long lastEtag, bool isTombstone)
            {
                if (isTombstone)
                {
                    LastProcessedTombstoneEtag = lastEtag;
                }
                else
                {
                    LastProcessedDocumentEtag = lastEtag;
                }
            }
        }
    }

    public class IndexesProgress
    {
        public List<IndexProgress> Results { get; set; }
    }
}
