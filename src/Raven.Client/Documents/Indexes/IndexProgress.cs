using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Raven.Client.Documents.Indexes
{
    public class IndexProgress
    {
        public string Name { get; set; }

        public IndexType Type { get; set; }

        public Dictionary<string, CollectionStats> Collections { get; set; }

        public bool IsStale { get; set; }

        public class CollectionStats
        {
            public long LastProcessedDocumentEtag { get; set; }

            public long NumberOfDocumentsToProcess { get; set; }

            public long TotalNumberOfDocuments { get; set; }

            public long LastProcessedTombstoneEtag { get; set; }

            public long NumberOfTombstonesToProcess { get; set; }

            public long TotalNumberOfTombstones { get; set; }

            internal void UpdateLastEtag(long lastEtag, bool isTombsone)
            {
                if (isTombsone)
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
