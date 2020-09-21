using System.Diagnostics;
using Raven.Client;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public abstract class ExtractedItem
    {
        protected ExtractedItem()
        {
            
        }

        protected ExtractedItem(Document document, string collection, EtlItemType type)
        {
            DocumentId = document.Id;
            Etag = document.Etag;
            Document = document;
            Collection = collection;
            ChangeVector = document.ChangeVector;
            Type = type;

            if (collection == null && type == EtlItemType.Document &&
                document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                metadata.TryGet(Constants.Documents.Metadata.Collection, out LazyStringValue docCollection))
                CollectionFromMetadata = docCollection;
        }

        protected ExtractedItem(Tombstone tombstone, string collection, EtlItemType type)
        {
            Etag = tombstone.Etag;

            Debug.Assert(tombstone.Type == Tombstone.TombstoneType.Document || tombstone.Type == Tombstone.TombstoneType.Attachment);
            DocumentId = tombstone.LowerId;
            Collection = collection;

            IsDelete = true;
            ChangeVector = tombstone.ChangeVector;
            Type = type;

            if (Collection == null)
                CollectionFromMetadata = tombstone.Collection;
        }

        public Document Document { get; set; }

        public LazyStringValue DocumentId { get; protected set; }

        public long Etag { get; protected set; }

        public string ChangeVector;

        public bool IsDelete { get; protected set; }

        public string Collection { get; set; }

        public LazyStringValue CollectionFromMetadata { get; }

        public EtlItemType Type { get; protected set; }

        public BlittableJsonReaderObject CounterGroupDocument { get; protected set; }
        
        public TimeSeriesSegmentEntry TimeSeriesSegmentEntry { get; protected set; }

        public TimeSeriesDeletedRangeItem TimeSeriesDeletedRangeItem { get; protected set; }
        
        public LazyStringValue CounterTombstoneId { get; protected set; }
    }
}
