using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(string fieldName, int fragmentLength, int fragmentCount, out Highlightings highlightings)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, null, out highlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(string fieldName, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, options, out highlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, out Highlightings highlightings)
        {
            Highlight(path.ToPropertyPath(), fragmentLength, fragmentCount, null, out highlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings)
        {
            Highlight(path.ToPropertyPath(), fragmentLength, fragmentCount, options, out highlightings);
            return this;
        }
    }
}
