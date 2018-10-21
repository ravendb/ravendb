using System;
using System.Linq.Expressions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface ISuggestionBuilder<T>
    {
        ISuggestionOperations<T> ByField(string fieldName, string term);

        ISuggestionOperations<T> ByField(string fieldName, string[] terms);

        ISuggestionOperations<T> ByField(Expression<Func<T, object>> path, string term);

        ISuggestionOperations<T> ByField(Expression<Func<T, object>> path, string[] terms);
    }

    public interface ISuggestionOperations<T>
    {
        ISuggestionOperations<T> WithDisplayName(string displayName);

        ISuggestionOperations<T> WithOptions(SuggestionOptions options);
    }

    internal class SuggestionBuilder<T> : ISuggestionBuilder<T>, ISuggestionOperations<T>
    {
        private SuggestionWithTerm _term;
        private SuggestionWithTerms _terms;

        public ISuggestionOperations<T> WithDisplayName(string displayName)
        {
            Suggestion.DisplayField = displayName;

            return this;
        }

        public ISuggestionOperations<T> ByField(string fieldName, string term)
        {
            if (fieldName == null)
                throw new ArgumentNullException(nameof(fieldName));
            if (term == null)
                throw new ArgumentNullException(nameof(term));

            _term = new SuggestionWithTerm(fieldName)
            {
                Term = term
            };

            return this;
        }

        public ISuggestionOperations<T> ByField(string fieldName, string[] terms)
        {
            if (fieldName == null)
                throw new ArgumentNullException(nameof(fieldName));
            if (terms == null)
                throw new ArgumentNullException(nameof(terms));
            if (terms.Length == 0)
                throw new ArgumentException("Value cannot be an empty collection.", nameof(terms));

            _terms = new SuggestionWithTerms(fieldName)
            {
                Terms = terms
            };

            return this;
        }

        public ISuggestionOperations<T> ByField(Expression<Func<T, object>> path, string term)
        {
            return ByField(path.ToPropertyPath(), term);
        }

        public ISuggestionOperations<T> ByField(Expression<Func<T, object>> path, string[] terms)
        {
            return ByField(path.ToPropertyPath(), terms);
        }

        public ISuggestionOperations<T> WithOptions(SuggestionOptions options)
        {
            Suggestion.Options = options;

            return this;
        }

        internal SuggestionBase Suggestion
        {
            get
            {
                if (_term != null)
                    return _term;

                return _terms;
            }
        }
    }
}
