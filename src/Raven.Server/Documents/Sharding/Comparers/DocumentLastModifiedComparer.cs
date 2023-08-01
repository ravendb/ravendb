using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Comparers;

public sealed class DocumentLastModifiedComparer : IComparer<BlittableJsonReaderObject>
{
    public static readonly DocumentLastModifiedComparer Throwing = new(throwIfCannotExtract: true);

    public static readonly DocumentLastModifiedComparer NotThrowing = new(throwIfCannotExtract: false);

    private readonly bool _throwIfCannotExtract;

    private DocumentLastModifiedComparer(bool throwIfCannotExtract)
    {
        _throwIfCannotExtract = throwIfCannotExtract;
    }

    public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
    {
        if (TryGetLastModified(x, out var xLastModified) == false && _throwIfCannotExtract)
            ThrowInvalidMissingLastModified();

        if (TryGetLastModified(y, out var yLastModified) == false && _throwIfCannotExtract)
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
