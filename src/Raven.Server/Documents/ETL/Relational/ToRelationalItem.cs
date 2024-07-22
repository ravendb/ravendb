using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Relational;


public sealed class ToRelationalItem : ExtractedItem 
{
    public ToRelationalItem(ToRelationalItem item)
    {
        Etag = item.Etag;
        DocumentId = item.DocumentId;
        Document = item.Document;
        IsDelete = item.IsDelete;
        Collection = item.Collection;
        ChangeVector = item.ChangeVector;
    }

    public ToRelationalItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
    {
    }

    public ToRelationalItem(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
    {
    }

    public List<RelationalColumn> Columns { get; set; }
}
