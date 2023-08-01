using System.Collections.Generic;

namespace Raven.Server.Documents.Sharding.Streaming.Comparers;

public sealed class StreamDocumentByLastModifiedComparer : Comparer<ShardStreamItem<Document>>
{
    public static StreamDocumentByLastModifiedComparer Instance = new StreamDocumentByLastModifiedComparer();
        
    public override int Compare(ShardStreamItem<Document> x, ShardStreamItem<Document> y)
    {
        return DocumentByLastModifiedDescComparer.Instance.Compare(x.Item, y.Item);
    }
}
