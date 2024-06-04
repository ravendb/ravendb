using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlItem : ExtractedItem
    {
        public RavenEtlItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
           
        }

        public RavenEtlItem(Tombstone tombstone, string collection, EtlItemType type) : base(tombstone, collection, type)
        {
            if (tombstone.Type == Tombstone.TombstoneType.Attachment)
            {
                AttachmentTombstoneId = tombstone.LowerId;
            }
        }

        public RavenEtlItem(CounterGroupDetail counter, string collection)
        {
            DocumentId = counter.DocumentId;
            Etag = counter.Etag;
            Collection = collection;
            ChangeVector = counter.ChangeVector;
            Type = EtlItemType.CounterGroup;
            CounterGroupDocument = counter.Values;
        }
        
        public RavenEtlItem(TimeSeriesSegmentEntry timeSeriesSegmentEntry, string collection)
        {
            DocumentId = timeSeriesSegmentEntry.DocId;
            Etag = timeSeriesSegmentEntry.Etag;
            Collection = collection;
            ChangeVector = timeSeriesSegmentEntry.ChangeVector;
            Type = EtlItemType.TimeSeries;
            TimeSeriesSegmentEntry = timeSeriesSegmentEntry;
        }

        public RavenEtlItem(TimeSeriesDeletedRangeItem deletedRange, string collection)
        {
            Etag = deletedRange.Etag;
            ChangeVector = deletedRange.ChangeVector;
            Collection = collection;
            Type = EtlItemType.TimeSeries;
            IsDelete = true;
            
            TimeSeriesDeletedRangeItem = deletedRange;

        }

        public LazyStringValue AttachmentTombstoneId { get; protected set; }

        public bool IsAttachmentTombstone => AttachmentTombstoneId != null;
    }
}
