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

        protected ExtractedItem(Document document, string collection)
        {
            DocumentKey = document.Key;
            Etag = document.Etag;
            Document = document;
            Collection = collection;

            if (collection == null)
            {
                CalculatedCollectionName = new Lazy<LazyStringValue>(() =>
                {
                    if (document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                    {
                        if (metadata.TryGet(Constants.Documents.Metadata.Collection, out LazyStringValue docCollection))
                        {
                            return docCollection;
                        }
                    }
                    return null;
                });
            }
        }

        protected ExtractedItem(DocumentTombstone tombstone, string collection)
        {
            Etag = tombstone.Etag;
            DocumentKey = tombstone.LoweredKey;
            IsDelete = true;
            Collection = collection;

            if (collection == null)
                CalculatedCollectionName = new Lazy<LazyStringValue>(() => tombstone.Collection);
        }

        public Document Document { get; protected set; }

        public LazyStringValue DocumentKey { get; protected set; }

        public long Etag { get; protected set; }

        public bool IsDelete { get; protected set; }

        public string Collection { get; protected set; }

        public Lazy<LazyStringValue> CalculatedCollectionName { get; }
    }
}