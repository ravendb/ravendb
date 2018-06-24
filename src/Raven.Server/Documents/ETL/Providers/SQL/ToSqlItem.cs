using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class ToSqlItem : ExtractedItem
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

        public ToSqlItem(Document document, string collection) : base(document, collection)
        {
        }

        public ToSqlItem(Tombstone tombstone, string collection) : base(tombstone, collection)
        {
        }

        public List<SqlColumn> Columns { get; set; }
    }
}
