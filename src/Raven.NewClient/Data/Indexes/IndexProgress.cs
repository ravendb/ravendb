using System.Collections.Generic;

namespace Raven.NewClient.Data.Indexes
{
    public class IndexProgress
    {
        public int Id { get; set; }

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
        }
    }
}