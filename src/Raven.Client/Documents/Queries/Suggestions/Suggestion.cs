namespace Raven.Client.Documents.Queries.Suggestions
{
    /// <inheritdoc />
    public sealed class SuggestionWithTerm : SuggestionBase
    {
        /// <inheritdoc />
        public SuggestionWithTerm(string field)
            : base(field)
        {
        }

        /// <summary>
        /// The term for which to get suggested similar terms.
        /// </summary>
        public string Term { get; set; }

    }

    /// <inheritdoc />
    public sealed class SuggestionWithTerms : SuggestionBase
    {
        /// <inheritdoc />
        public SuggestionWithTerms(string field)
            : base(field)
        {
        }

        /// <summary>
        /// List of terms for which to get suggested similar terms
        /// </summary>
        public string[] Terms { get; set; }
    }

    /// <summary>
    /// Given a string term (or terms), the Suggestion feature will offer similar terms from your data.
    /// Word similarities are found using string distance algorithms.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Session.Querying.SuggestionsQuery"/>
    public abstract class SuggestionBase
    {
        /// <inheritdoc cref="SuggestionBase"/>
        /// <param name="field">The index field in which to search for similar terms. </param>
        protected SuggestionBase(string field)
        {
            Field = field;
        }

        /// <summary>
        /// Field on which perform term-search.
        /// </summary>
        public string Field { get; set; }

        /// <summary>
        /// A custom name for the suggestions result (optional).
        /// </summary>
        public string DisplayField { get; set; }

        /// <summary>
        /// Non-default options to use in the operation (optional).
        /// </summary>
        public SuggestionOptions Options { get; set; }
    }
}
