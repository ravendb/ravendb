using System;
using System.Collections.Generic;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentTransactionCache
    {
        public long LastDocumentEtag;
        public long LastTombstoneEtag;
        public long LastCounterEtag;
        public long LastTimeSeriesEtag;
        public long LastConflictEtag;
        public long LastRevisionsEtag;
        public long LastAttachmentsEtag;

        public long LastEtag;

        public class CollectionCache
        {
            public long LastDocumentEtag;
            public long LastTombstoneEtag;
            public string LastChangeVector;
        }

        public readonly Dictionary<string, CollectionCache> LastEtagsByCollection = new Dictionary<string, CollectionCache>(StringComparer.OrdinalIgnoreCase);
    }
}
