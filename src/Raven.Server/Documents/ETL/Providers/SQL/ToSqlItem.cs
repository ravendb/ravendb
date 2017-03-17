using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class ToSqlItem : ExtractedItem
    {
        public ToSqlItem(ToSqlItem item)
        {
            Etag = item.Etag;
            DocumentKey = item.DocumentKey;
            Document = item.Document;
            IsDelete = item.IsDelete;
        }


        public ToSqlItem(Document document) : base(document)
        {
        }

        public ToSqlItem(DocumentTombstone tombstone) : base(tombstone)
        {
        }

        public List<SqlColumn> Columns { get; set; }
    }
}