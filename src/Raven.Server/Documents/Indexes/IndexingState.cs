namespace Raven.Server.Documents.Indexes
{
    public class IndexingState
    {
        public IndexingState(bool isStale, long lastProcessedEtag, long? lastProcessedCompareExchangeReferenceEtag, long? lastProcessedCompareExchangeReferenceTombstoneEtag)
        {
            IsStale = isStale;
            LastProcessedEtag = lastProcessedEtag;
            LastProcessedCompareExchangeReferenceEtag = lastProcessedCompareExchangeReferenceEtag;
            LastProcessedCompareExchangeReferenceTombstoneEtag = lastProcessedCompareExchangeReferenceTombstoneEtag;
        }

        public readonly bool IsStale;

        public readonly long LastProcessedEtag;

        public long? LastProcessedCompareExchangeReferenceEtag;

        public long? LastProcessedCompareExchangeReferenceTombstoneEtag;
    }
}
