using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public abstract class ExtractedItem
    {
        protected ExtractedItem()
        {
            
        }

        protected ExtractedItem(Document document)
        {
            DocumentKey = document.Key;
            Etag = document.Etag;
            Document = document;
        }

        protected ExtractedItem(DocumentTombstone tombstone)
        {
            Etag = tombstone.Etag;
            DocumentKey = tombstone.LoweredKey;
            IsDelete = true;
        }

        public Document Document { get; protected set; }

        public string DocumentKey { get; protected set; }

        public long Etag { get; protected set; }

        public bool IsDelete { get; protected set; }
    }
}