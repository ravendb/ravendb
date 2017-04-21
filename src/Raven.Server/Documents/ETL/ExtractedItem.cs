using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public abstract class ExtractedItem
    {
        protected ExtractedItem()
        {
            
        }

        protected ExtractedItem(Document document, string collection)
        {
            DocumentKey = document.Key;
            Etag = document.Etag;
            Document = document;
            Collection = collection;
        }

        protected ExtractedItem(DocumentTombstone tombstone, string collection)
        {
            Etag = tombstone.Etag;
            DocumentKey = tombstone.LoweredKey;
            IsDelete = true;
            Collection = collection;
        }

        public Document Document { get; protected set; }

        public LazyStringValue DocumentKey { get; protected set; }

        public long Etag { get; protected set; }

        public bool IsDelete { get; protected set; }

        public string Collection { get; protected set; }
    }
}