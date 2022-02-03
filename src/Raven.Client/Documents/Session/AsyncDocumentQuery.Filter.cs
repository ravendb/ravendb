using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session;

public partial class AsyncDocumentQuery<T>
{
    IAsyncDocumentQuery<T> IAsyncDocumentQuery<T>.Filter(Action<IFilterFactory<T>> builder, int limit)
    {
        using (SetFilterMode(true))
        {
            var f = new FilterFactory<T>(this, limit);
            builder.Invoke(f);
        }
        
        return this;
    }
}
