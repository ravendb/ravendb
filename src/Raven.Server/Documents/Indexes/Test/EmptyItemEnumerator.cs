using System;
using System.Collections;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Test;

public class EmptyItemEnumerator : IIndexedItemEnumerator
{
    public void Dispose()
    {
    }

    public bool MoveNext(DocumentsOperationContext ctx, out IEnumerable resultsOfCurrentDocument, out long? etag)
    {
        resultsOfCurrentDocument = default;
        etag = default;
        return false;
    }

    public void OnError()
    {
        
    }

    public IndexItem Current => default;
}
