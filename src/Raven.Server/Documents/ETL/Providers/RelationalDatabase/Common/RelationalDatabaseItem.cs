using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;

public sealed class RelationalDatabaseItem : ExtractedItem 
{
    public RelationalDatabaseItem(RelationalDatabaseItem item)
    {
        Etag = item.Etag;
        DocumentId = item.DocumentId;
        Document = item.Document;
        IsDelete = item.IsDelete;
        Collection = item.Collection;
        ChangeVector = item.ChangeVector;
    }

    public RelationalDatabaseItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
    {
    }

    public RelationalDatabaseItem(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
    {
    }

    public List<RelationalDatabaseColumn> Columns { get; set; }
}
