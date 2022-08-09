using System;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected Explanations Explanations = new Explanations();

        protected ExplanationToken ExplanationToken;

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
