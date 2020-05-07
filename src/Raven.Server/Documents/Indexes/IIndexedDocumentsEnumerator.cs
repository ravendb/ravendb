using System;
using System.Collections;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public interface IIndexedItemEnumerator : IDisposable
    {
        bool MoveNext(DocumentsOperationContext ctx, out IEnumerable resultsOfCurrentDocument, out long? etag);

        void OnError();

        IndexItem Current { get; }
    }
}
