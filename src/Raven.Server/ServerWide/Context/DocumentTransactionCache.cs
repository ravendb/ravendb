using System;
using System.Collections.Immutable;

namespace Raven.Server.ServerWide.Context
{
    public sealed class DocumentTransactionCache
    {
        // set once the per-collection entries were populated by a full scan (or built incrementally
        // on top of a fully computed cache), allowing the next commit to reuse them instead of
        // reading the last document of every collection again
        public bool FullyComputed;

        public long LastDocumentEtag;
        public long LastTombstoneEtag;
        public long LastCounterEtag;
        public long LastTimeSeriesEtag;
        public long LastConflictEtag;
        public long LastRevisionsEtag;
        public long LastAttachmentsEtag;

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
