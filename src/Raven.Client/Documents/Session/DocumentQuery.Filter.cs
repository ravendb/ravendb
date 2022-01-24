using System;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.MoreLikeThis;

namespace Raven.Client.Documents.Session;

public partial class DocumentQuery<T>
{
    IDocumentQuery<T> IDocumentQuery<T>.Filter(Action<IFilterFactory<T>> builder)
    {
        TurnOnFilter();
        var f = new FilterFactory<T>(this);
        builder.Invoke(f);
        TurnOffFilter();
        return this;
    }
    
    IDocumentQuery<T> IDocumentQuery<T>.ScanLimit(int limit)
    {
        AddScanLimit(limit);
        return this;
    }
}
