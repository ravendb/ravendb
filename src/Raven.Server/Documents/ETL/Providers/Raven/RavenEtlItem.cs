using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlItem : ExtractedItem
    {
        public RavenEtlItem(Document document, string collection) : base(document, collection)
        {
            if (collection != null)
                return;

            if (document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                if (metadata.TryGet(Constants.Documents.Metadata.Collection, out LazyStringValue docCollection))
                {
                    CollectionFromMetadata = docCollection;
                }
            }
        }

        public RavenEtlItem(DocumentTombstone tombstone, string collection) : base(tombstone, collection)
        {
            if (collection != null)
                return;

            CollectionFromMetadata = tombstone.Collection;
        }

        public LazyStringValue CollectionFromMetadata { get; }
    }
}