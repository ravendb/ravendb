using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class ToS3Item : ExtractedItem
    {
        public ToS3Item(ToS3Item item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public ToS3Item(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public ToS3Item(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
        {
        }

        public List<SqlColumn> Properties { get; set; }
    }
}
