using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Queries.Suggestion;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.Suggest(Expression<Func<T, object>> path, string term, SuggestionOptions options)
        {
            Suggest(path.ToPropertyPath(), term, options);
            return new AsyncSuggestionDocumentQuery<T>(this);
        }

        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.Suggest(string fieldName, string term, SuggestionOptions options)
        {
            Suggest(fieldName, term, options);
            return new AsyncSuggestionDocumentQuery<T>(this);
        }

        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.Suggest(Expression<Func<T, object>> path, string[] terms, SuggestionOptions options)
        {
            Suggest(path.ToPropertyPath(), terms, options);
            return new AsyncSuggestionDocumentQuery<T>(this);
        }

        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.Suggest(string fieldName, string[] terms, SuggestionOptions options)
        {
            Suggest(fieldName, terms, options);
            return new AsyncSuggestionDocumentQuery<T>(this);
        }
    }
}
