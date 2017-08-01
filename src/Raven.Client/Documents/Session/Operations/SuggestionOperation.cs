using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries.Suggestion;

namespace Raven.Client.Documents.Session.Operations
{

    internal class SuggestionOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly SuggestionQuery _query;

        private SuggestionQueryResult _result;

        public SuggestionOperation(InMemoryDocumentSessionOperations session, SuggestionQuery query)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _query = query ?? throw new ArgumentNullException(nameof(query));
        }

        public SuggestionCommand CreateRequest()
        {
            _session.IncrementRequestCount();

            return new SuggestionCommand(_session.Conventions, _session.Context, _query);
        }

        public void SetResult(SuggestionQueryResult result)
        {
            _result = result;
        }

        public SuggestionQueryResult Complete()
        {
            return _result;
        }
    }
}
