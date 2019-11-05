using System;
using System.Collections;

namespace Raven.Server.Documents.Indexes
{
    public interface IIndexedItemEnumerator : IDisposable
    {
        bool MoveNext(out IEnumerable resultsOfCurrentDocument);

        void OnError();

        IndexItem Current { get; }
    }
}
