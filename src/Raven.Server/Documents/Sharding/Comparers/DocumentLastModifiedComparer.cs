using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Comparers;

public class DocumentLastModifiedComparer : IComparer<BlittableJsonReaderObject>
{
    public static readonly DocumentLastModifiedComparer Instance = new();

    private DocumentLastModifiedComparer()
    {
    }

    public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
    {
        if (TryGetLastModified(y, out var yLastModified) == false)
            ThrowInvalidMissingLastModified();

        if (TryGetLastModified(x, out var xLastModified) == false)
            ThrowInvalidMissingLastModified();

        return xLastModified.CompareTo(yLastModified);

        static bool TryGetLastModified(BlittableJsonReaderObject json, out DateTime lastModified)
        {
            if (json == null)
            {
                lastModified = default;
                return false;
            }

            if (json.TryGetMetadata(out var metadata) == false)
            {
                lastModified = default;
                return false;
            }

            if (metadata.TryGetLastModified(out lastModified) == false)
            {
                lastModified = default;
                return false;
            }

            return true;
        }

        static void ThrowInvalidMissingLastModified()
        {
            throw new InvalidOperationException($"Document does not contain '{Constants.Documents.Metadata.LastModified}' field.");
        }
    }
}
