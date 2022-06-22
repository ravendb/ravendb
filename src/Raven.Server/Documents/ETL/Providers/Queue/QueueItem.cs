using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Queue
{
    public class QueueItem : ExtractedItem
    {
        public QueueItem(QueueItem item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public QueueItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public BlittableJsonReaderObject TransformationResult { get; set; }

        public CloudEventAttributes Attributes { get; set; }
    }
}
