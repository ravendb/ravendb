using System.Collections.Generic;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected LinkedList<HighlightingToken> HighlightingTokens = new LinkedList<HighlightingToken>();

        protected QueryHighlightings QueryHighlightings = new QueryHighlightings();

        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings)
        {
            highlightings = QueryHighlightings.Add(fieldName);

            var optionsParameterName = options != null ? AddQueryParameter(options) : null;

            HighlightingTokens.AddLast(HighlightingToken.Create(fieldName, fragmentLength, fragmentCount, optionsParameterName));
        }
    }
}
