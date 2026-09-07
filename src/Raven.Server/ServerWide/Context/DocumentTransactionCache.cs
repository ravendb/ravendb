using System;
using System.Collections.Generic;
using Raven.Server.Documents;

namespace Raven.Server.ServerWide.Context
{
    public sealed class DocumentTransactionCache
    {
        public Dictionary<string, CollectionName> Collections;

        internal bool Published;

        public static DocumentTransactionCache GetForUpdate(Voron.Impl.LowLevelTransaction tx)
        {
            if (tx.TryGetClientState(out DocumentTransactionCache cache) == false)
            {
                cache = new DocumentTransactionCache();
            }   
            else if (cache.Published is false)
            {
                return cache; // already private to this transaction
            }
            
            cache = (DocumentTransactionCache)cache.MemberwiseClone();
            cache.Published = false;
            tx.UpdateClientState(cache);
            return cache;
        }

        public long LastDocumentEtag;
        public long LastTombstoneEtag;
        public long LastCounterEtag;
        public long LastTimeSeriesEtag;
        public long LastConflictEtag;
        public long LastRevisionsEtag;
        public long LastAttachmentsEtag;
        public long ConflictsCount;
        public long RevisionsCount;
        public long LastEtag;

        public sealed class CollectionCache
        {
            public long LastDocumentEtag;
            public long LastTombstoneEtag;
            public string LastChangeVector;
        }

        public readonly Dictionary<string, CollectionCache> LastEtagsByCollection = new(StringComparer.OrdinalIgnoreCase);
    }
}
