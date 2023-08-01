using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public sealed class ToSqlItem : ExtractedItem
    {
        public ToSqlItem(ToSqlItem item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public ToSqlItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public ToSqlItem(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
        {
        }

        public List<SqlColumn> Columns { get; set; }
    }
}
