using System.Diagnostics;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Schemas;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public sealed class RavenEtlItem : ExtractedItem
    {
        public RavenEtlItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
           
        }

        public RavenEtlItem(Tombstone tombstone, string collection, EtlItemType type) : base(tombstone, collection, type)
        {
            if (tombstone.Type == Tombstone.TombstoneType.Attachment)
            {
                Debug.Assert(false, "tombstone.Type == Tombstone.TombstoneType.Attachment");
            }
        }
        //AttachmentTombstoneReplicationItem

        public RavenEtlItem(DocumentsOperationContext context, AttachmentTombstoneReplicationItem attachment)
        {
            DocumentId = context.GetLazyString(attachment.Key.ToString());

            //AttachmentTombstoneId = attachment.Key;
            //IsAttachmentTombstone = true;
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

        ////TODO: egor move this to separate class
        //public Slice AttachmentTombstoneId { get; }

        //public bool IsAttachmentTombstone;
    }
}
