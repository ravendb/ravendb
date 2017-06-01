using Raven.Client.Documents.Replication.Messages;
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
            DocumentId = document.Id;
            Etag = document.Etag;
            Document = document;
            Collection = collection;
            ChangeVector = document.ChangeVector;
        }

        protected ExtractedItem(DocumentTombstone tombstone, string collection)
        {
            Etag = tombstone.Etag;
            DocumentId = tombstone.LowerId;
            IsDelete = true;
            Collection = collection;
            ChangeVector = tombstone.ChangeVector;
        }

        public Document Document { get; protected set; }

        public LazyStringValue DocumentId { get; protected set; }

        public long Etag { get; protected set; }

        public ChangeVectorEntry[] ChangeVector;

        public bool IsDelete { get; protected set; }

        public string Collection { get; protected set; }
    }
}