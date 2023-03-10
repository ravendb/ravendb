using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Comparers;

public class DocumentLastModifiedComparer : IComparer<BlittableJsonReaderObject>
{
    public static DocumentLastModifiedComparer Instance = new();

    private DocumentLastModifiedComparer()
    {
    }

    public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
    {
        return y.GetMetadata().GetLastModified().CompareTo(x.GetMetadata().GetLastModified());
    }
}
