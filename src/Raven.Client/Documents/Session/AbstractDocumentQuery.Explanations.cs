using System;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected Explanations Explanations = new Explanations();

        protected ExplanationToken ExplanationToken;

        /// <summary>
        /// Explanations gives context how document was matched by query and provide information about how the score was calculated.
        /// </summary>
        /// <param name="options" cref="ExplanationOptions">Additional explanation configuration.</param>
        /// <param name="explanations">Out parameter where explanations will be returned.</param>
        /// <exception cref="InvalidOperationException">Explanations should be included only once in the query.</exception>
        /// <inheritdoc cref="DocumentationUrls.ExplanationDocumentation"/>
        public void IncludeExplanations(ExplanationOptions options, out Explanations explanations)
        {
            if (ExplanationToken != null)
                throw new InvalidOperationException($"Duplicate '{nameof(IncludeExplanations)}' method calls are forbidden.");

            var optionsParameterName = options != null ? AddQueryParameter(options) : null;
            ExplanationToken = ExplanationToken.Create(optionsParameterName);
            Explanations.ShouldBeIncluded = true;
            explanations = Explanations;
        }

    }
}
