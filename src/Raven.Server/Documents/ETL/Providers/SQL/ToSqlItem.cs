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

        public ToSqlItem(Document document)
        {
            Etag = document.Etag;
            DocumentKey = document.Key;
            Document = document;
        }
        
        public ToSqlItem(DocumentTombstone tombstone)
        {
            Etag = tombstone.Etag;
            DocumentKey = tombstone.Key;
            IsDelete = true;
        }

        public Document Document { get; private set; }

        public string DocumentKey { get; private set; } // TODO arek - LazyStringValue

        public List<SqlColumn> Columns { get; set; }
    }
}