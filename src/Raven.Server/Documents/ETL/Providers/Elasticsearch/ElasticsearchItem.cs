namespace Raven.Server.Documents.ETL.Providers.Elasticsearch
{
    public class ElasticsearchItem : ExtractedItem
    {
        public ElasticsearchItem(ElasticsearchItem item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public ElasticsearchItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public ElasticsearchItem(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
        {
        }
        
        public ElasticsearchProperty Property { get; set; }
    }
}
