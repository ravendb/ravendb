using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.SQL;

namespace Raven.Server.Documents.ETL.Providers.Parquet
{
    public class ToParquetItem : ExtractedItem
    {
        public ToParquetItem(ToParquetItem item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public ToParquetItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public ToParquetItem(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
        {
        }

        public List<SqlColumn> Properties { get; set; }
    }
}
