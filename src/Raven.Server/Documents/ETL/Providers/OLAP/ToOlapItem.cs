using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.SQL;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class ToOlapItem : ExtractedItem
    {
        public ToOlapItem(ToOlapItem item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public ToOlapItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public List<SqlColumn> Properties { get; set; }
    }
}
