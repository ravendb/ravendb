using System;
using System.Collections.Immutable;

namespace Raven.Server.ServerWide.Context
{
    public sealed class DocumentTransactionCache
    {
        // false on the initial empty instance; set to true by every compute (full or incremental). an
        // incremental cache built from a fully-computed base stays complete, so the flag propagates.
        public bool FullyComputed;

        public long LastDocumentEtag;
        public long LastTombstoneEtag;
        public long LastCounterEtag;
        public long LastTimeSeriesEtag;
        public long LastConflictEtag;
        public long LastRevisionsEtag;
        public long LastAttachmentsEtag;
        public long ConflictsCount;
        public long LastEtag;

        public sealed class CollectionCache
        {
            public long LastDocumentEtag;
            public long LastTombstoneEtag;
            public string LastChangeVector;
        }

        public ImmutableDictionary<string, CollectionCache> LastEtagsByCollection = ImmutableDictionary.Create<string, CollectionCache>(StringComparer.OrdinalIgnoreCase);
    }
}
