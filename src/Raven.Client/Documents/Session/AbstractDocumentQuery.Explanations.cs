using System;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected Explanations Explanations;

        protected ExplanationToken Explanation;

        public void Explain(ExplanationOptions options, out Explanations explanations)
        {
            if (Explanation != null)
                throw new InvalidOperationException($"Duplicate '{nameof(Explain)}' method calls are forbidden.");

            var optionsParameterName = options != null ? AddQueryParameter(options) : null;
            Explanation = ExplanationToken.Create(optionsParameterName);
            Explanations = explanations = new Explanations();
        }
    }
}
