using System.Collections.Generic;

namespace Raven.Server.Documents.Sharding.Streaming.Comparers;

public class DocumentByLastModifiedComparer : Comparer<Document>
{
    public static DocumentByLastModifiedComparer Instance = new DocumentByLastModifiedComparer();
        
    public override int Compare(Document x, Document y)
    {
        var diff = x.LastModified.Ticks - y.LastModified.Ticks;
        if (diff > 0)
            return 1;
        if (diff < 0)
            return -1;
        return 0;
    }
}
