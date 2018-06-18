using System;
using Raven.Client;
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

            if (collection == null &&
                document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                metadata.TryGet(Constants.Documents.Metadata.Collection, out LazyStringValue docCollection))
                CollectionFromMetadata = docCollection;
        }

        protected ExtractedItem(Tombstone tombstone, string collection, EtlItemType type)
        {
            Etag = tombstone.Etag;

            switch (type)
            {
                case EtlItemType.Document:
                    DocumentId = tombstone.LowerId;
                    break;
                case EtlItemType.Counter:
                    throw new NotImplementedException("TODO arek");
            }

            IsDelete = true;
            Collection = collection;
            ChangeVector = tombstone.ChangeVector;
            Type = type;

            if (collection == null)
                CollectionFromMetadata = tombstone.Collection;
        }

        public Document Document { get; protected set; }

        public LazyStringValue DocumentId { get; protected set; }

        public long Etag { get; protected set; }

        public string ChangeVector;

        public bool IsDelete { get; protected set; }

        public string Collection { get; protected set; }

        public LazyStringValue CollectionFromMetadata { get; }

        public EtlItemType Type { get; protected set; }

        public string CounterName { get; protected set; }

        public long CounterValue { get; protected set; }
    }
}
