using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;

public sealed class ToRelationalDatabaseItem : ExtractedItem 
{
    public ToRelationalDatabaseItem(ToRelationalDatabaseItem item)
    {
        Etag = item.Etag;
        DocumentId = item.DocumentId;
        Document = item.Document;
        IsDelete = item.IsDelete;
        Collection = item.Collection;
        ChangeVector = item.ChangeVector;
    }

    public ToRelationalDatabaseItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
    {
    }

    public ToRelationalDatabaseItem(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
    {
    }

    public List<RelationalDatabaseColumn> Columns { get; set; }
}
