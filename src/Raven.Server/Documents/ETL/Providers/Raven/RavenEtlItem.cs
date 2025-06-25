using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public sealed class RavenEtlItem : ExtractedItem
    {
        public RavenEtlItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
           
        }

        public RavenEtlItem(Tombstone tombstone, string collection, EtlItemType type) : base(tombstone, collection, type)
        {
        }

        public RavenEtlItem(DocumentsOperationContext context, AttachmentTombstoneReplicationItem attachment)
        {
            DocumentId = context.GetLazyString(attachment.Key.ToString());

            Collection = "__undefined";
            Type = EtlItemType.Document;
            IsDelete = true;

            Etag = attachment.Etag;
            ChangeVector = attachment.ChangeVector;

            AttachmentTombstone = attachment;
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
    }
}
