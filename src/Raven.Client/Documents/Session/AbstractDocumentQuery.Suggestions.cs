using System;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        public void SuggestUsing(SuggestionBase suggestion)
        {
            if (suggestion == null)
                throw new ArgumentNullException(nameof(suggestion));

            AssertCanSuggest(suggestion);

            SuggestToken token;
            switch (suggestion)
            {
                case SuggestionWithTerm term:
                    token = SuggestToken.Create(term.Field, term.DisplayField, AddQueryParameter(term.Term), GetOptionsParameterName(term.Options));
                    break;
                case SuggestionWithTerms terms:
                    token = SuggestToken.Create(terms.Field, terms.DisplayField, AddQueryParameter(terms.Terms), GetOptionsParameterName(terms.Options));
                    break;
                default:
                    throw new NotSupportedException($"Unknown type of suggestion '{suggestion.GetType()}'");
            }

            SelectTokens.AddLast(token);
        }

        private string GetOptionsParameterName(SuggestionOptions options)
        {
            string optionsParameterName = null;
            if (options != null && options != SuggestionOptions.Default)
                optionsParameterName = AddQueryParameter(options);

            return optionsParameterName;
        }

        private void AssertCanSuggest(SuggestionBase suggestion)
        {
            if (WhereTokens.Count > 0)
                throw new InvalidOperationException("Cannot add suggest when WHERE statements are present.");

            if (SelectTokens.Count > 0)
            {
                if (SelectTokens.Last.Value is SuggestToken st)
                {
                    if (st.FieldName == suggestion.Field)
                        throw new InvalidOperationException("Cannot add suggest for the same field again.");
                }
                else
                    throw new InvalidOperationException("Cannot add suggest when SELECT statements are present.");
            }

            if (OrderByTokens.Count > 0)
                throw new InvalidOperationException("Cannot add suggest when ORDER BY statements are present.");
        }
    }
}
