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
        }

        public Dictionary<string, CollectionEtags> Collections = new Dictionary<string, CollectionEtags>();
        public Dictionary<string, Tree.ChunkDetails[]> ChunksByName = new Dictionary<string, Tree.ChunkDetails[]>();
    }
}
