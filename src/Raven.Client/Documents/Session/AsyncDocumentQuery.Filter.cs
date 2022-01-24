using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session;

public partial class AsyncDocumentQuery<T>
{
    IAsyncDocumentQuery<T> IAsyncDocumentQuery<T>.Filter(Action<IFilterFactory<T>> builder)
    {
        using (FilterModeScope(true))
        {
            var f = new AsyncFilterFactory<T>(this);
            builder.Invoke(f);
        }
        return this;
    }
    
    IAsyncDocumentQuery<T> IAsyncDocumentQuery<T>.ScanLimit(int limit)
    {
        AddScanLimit(limit);
        return this;
    }
}
