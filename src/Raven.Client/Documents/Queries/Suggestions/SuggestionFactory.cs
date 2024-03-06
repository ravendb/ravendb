using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface ISuggestionBuilder<T>
    {
        /// <inheritdoc cref="ISuggestionQuery{T}"/>
        /// <param name="fieldName">Field on which perform term-search.</param>
        /// <param name="term">The term for which to get suggested similar terms.</param>
        ISuggestionOperations<T> ByField(string fieldName, string term);

        /// <inheritdoc cref="ISuggestionQuery{T}"/>
        /// <param name="fieldName">Field on which perform term-search.</param>
        /// <param name="terms">List of terms for which to get suggested similar terms.</param>
        ISuggestionOperations<T> ByField(string fieldName, string[] terms);


        /// <inheritdoc cref="ISuggestionQuery{T}"/>
        /// <param name="path">Field on which perform term-search.</param>
        /// <param name="term">The term for which to get suggested similar terms.</param>
        ISuggestionOperations<T> ByField(Expression<Func<T, object>> path, string term);

        /// <inheritdoc cref="ISuggestionQuery{T}"/>
        /// <param name="path">Field on which perform term-search.</param>
        /// <param name="terms">List of terms for which to get suggested similar terms.</param>
        ISuggestionOperations<T> ByField(Expression<Func<T, object>> path, string[] terms);
    }

    public interface ISuggestionOperations<T>
    {
        /// <summary>
        /// A custom name for the suggestions result.
        /// </summary>
        /// <param name="displayName">Custom name.</param>
        ISuggestionOperations<T> WithDisplayName(string displayName);


        /// <summary>
        /// Non-default options to use in the operation.
        /// </summary>
        /// <param name="options"></param>
        ISuggestionOperations<T> WithOptions(SuggestionOptions options);
    }

    internal sealed class SuggestionBuilder<T> : ISuggestionBuilder<T>, ISuggestionOperations<T>
    {
        private readonly DocumentConventions _conventions;
        private SuggestionWithTerm _term;
        private SuggestionWithTerms _terms;

        public SuggestionBuilder(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        /// <inheritdoc/>
        public ISuggestionOperations<T> WithDisplayName(string displayName)
        {
            Suggestion.DisplayField = displayName;

            return this;
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public ISuggestionOperations<T> ByField(Expression<Func<T, object>> path, string term)
        {
            return ByField(path.ToPropertyPath(_conventions), term);
        }

        /// <inheritdoc/>
        public ISuggestionOperations<T> ByField(Expression<Func<T, object>> path, string[] terms)
        {
            return ByField(path.ToPropertyPath(_conventions), terms);
        }

        /// <inheritdoc/>
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
