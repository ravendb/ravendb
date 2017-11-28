using System;using System.Linq.Expressions;using Raven.Client.Documents.Queries.Suggestion;using Raven.Client.Extensions;namespace Raven.Client.Documents.Session
{
    public partial class DocumentQuery<T>
    {        ISuggestionDocumentQuery<T> IDocumentQuery<T>.Suggest(Expression<Func<T, object>> path, string term, SuggestionOptions options = null)        {            Suggest(path.ToPropertyPath(), term, options);            return new SuggestionDocumentQuery<T>(this);        }        ISuggestionDocumentQuery<T> IDocumentQuery<T>.Suggest(string fieldName, string term, SuggestionOptions options)        {            Suggest(fieldName, term, options);            return new SuggestionDocumentQuery<T>(this);        }        ISuggestionDocumentQuery<T> IDocumentQuery<T>.Suggest(Expression<Func<T, object>> path, string[] terms, SuggestionOptions options = null)        {            Suggest(path.ToPropertyPath(), terms, options);            return new SuggestionDocumentQuery<T>(this);        }        ISuggestionDocumentQuery<T> IDocumentQuery<T>.Suggest(string fieldName, string[] terms, SuggestionOptions options)
        {
            Suggest(fieldName, terms, options);
            return new SuggestionDocumentQuery<T>(this);
        }
    }
}
