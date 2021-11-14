using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public class ElasticSearchItem : ExtractedItem
    {
        public ElasticSearchItem(ElasticSearchItem item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public ElasticSearchItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public ElasticSearchItem(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
        {
        }

        public BlittableJsonReaderObject TransformationResult { get; set; }
    }
}
