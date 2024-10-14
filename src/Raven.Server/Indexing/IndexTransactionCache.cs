using System;
using System.Collections.Generic;
using Voron.Data.BTrees;

namespace Raven.Server.Indexing
{
    public class IndexTransactionCache
    {
        public class CollectionEtags
        {
            public long LastIndexedEtag;
            public long LastProcessedTombstoneEtag;
            public long LastProcessedTimeSeriesDeletedRangeEtag;
            public Dictionary<string, ReferenceCollectionEtags> LastReferencedEtags;
            public ReferenceCollectionEtags LastReferencedEtagsForCompareExchange;
        }

        public class ReferenceCollectionEtags
        {
            public long LastEtag;
            public long LastProcessedTombstoneEtag;
        }

        public class DirectoryFiles
        {
            public Dictionary<string, Tree.ChunkDetails[]> ChunksByName = new Dictionary<string, Tree.ChunkDetails[]>(StringComparer.OrdinalIgnoreCase);
        }

        public Dictionary<string, DirectoryFiles> DirectoriesByName = new Dictionary<string, DirectoryFiles>(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, CollectionEtags> Collections = new Dictionary<string, CollectionEtags>(StringComparer.OrdinalIgnoreCase);
    }
}
