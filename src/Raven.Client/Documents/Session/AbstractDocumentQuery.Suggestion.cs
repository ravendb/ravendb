using System;
using Raven.Client.Documents.Queries.Suggestion;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        public void Suggest(string fieldName, string term, SuggestionOptions options = null)
        {
            if (fieldName == null)
                throw new ArgumentNullException(nameof(fieldName));
            if (term == null)
                throw new ArgumentNullException(nameof(term));

            AssertCanSuggest();

            SelectTokens.AddLast(SuggestToken.Create(fieldName, AddQueryParameter(term), GetOptionsParameterName(options)));
        }

        public void Suggest(string fieldName, string[] terms, SuggestionOptions options = null)
        {
            if (fieldName == null)
                throw new ArgumentNullException(nameof(fieldName));
            if (terms == null)
                throw new ArgumentNullException(nameof(terms));
            if (terms.Length == 0)
                throw new ArgumentException("Value cannot be an empty collection.", nameof(terms));

            AssertCanSuggest();

            SelectTokens.AddLast(SuggestToken.Create(fieldName, AddQueryParameter(terms), GetOptionsParameterName(options)));
        }

        private string GetOptionsParameterName(SuggestionOptions options)
        {
            string optionsParameterName = null;
            if (options != null && options != SuggestionOptions.Default)
                optionsParameterName = AddQueryParameter(options);

            return optionsParameterName;
        }

        private void AssertCanSuggest()
        {
            if (WhereTokens.Count > 0)
                throw new InvalidOperationException("Cannot add suggest when WHERE statements are present.");

            if (SelectTokens.Count > 0)
                throw new InvalidOperationException("Cannot add suggest when SELECT statements are present.");

            if (OrderByTokens.Count > 0)
                throw new InvalidOperationException("Cannot add suggest when ORDER BY statements are present.");
        }
    }
}
