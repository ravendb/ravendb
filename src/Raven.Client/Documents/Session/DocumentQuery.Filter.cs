using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session;

public partial class DocumentQuery<T>
{
    IDocumentQuery<T> IDocumentQuery<T>.Filter(Action<IFilterFactory<T>> builder)
    {
        using (FilterModeScope(true))
        {
            var f = new FilterFactory<T>(this);
            builder.Invoke(f);
        }

        return this;
    }
    
    IDocumentQuery<T> IDocumentQuery<T>.ScanLimit(int limit)
    {
        AddScanLimit(limit);
        return this;
    }
}
