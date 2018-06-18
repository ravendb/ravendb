using Raven.Client.Documents.Operations.Counters;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlItem : ExtractedItem
    {
        public RavenEtlItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
           
        }

        public RavenEtlItem(Tombstone tombstone, string collection, EtlItemType type) : base(tombstone, collection, type)
        {
           
        }

        public RavenEtlItem(CounterDetail counter, string collection)
        {
            DocumentId = counter.LazyDocumentId;
            Etag = counter.Etag;
            Collection = collection;
            ChangeVector = counter.ChangeVector;
            Type = EtlItemType.Counter;
            CounterName = counter.CounterName;
            CounterValue = counter.TotalValue;
        }
    }
}
