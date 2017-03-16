using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlItem : ExtractedItem
    {
        public RavenEtlItem(Document document)
        {
            DocumentKey = document.Key;
            Etag = document.Etag;
            Document = document;
        }

        public RavenEtlItem(DocumentTombstone tombstone)
        {
            Etag = tombstone.Etag;
            DocumentKey = tombstone.Key;
            IsDelete = true;
        }

        public Document Document { get; private set; }

        public LazyStringValue DocumentKey { get; private set; }
    }
}