using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session;

public partial class AsyncDocumentQuery<T>
{
    IAsyncDocumentQuery<T> IAsyncDocumentQuery<T>.Filter(Action<IFilterFactory<T>> builder, int filterLimit)
    {
        using (SetFilterMode(true))
        {
            var f = new FilterFactory<T>(this, filterLimit);
            builder.Invoke(f);
        }
        
        return this;
    }
}
