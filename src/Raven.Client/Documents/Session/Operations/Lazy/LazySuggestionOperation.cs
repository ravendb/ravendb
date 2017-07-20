using System;
using System.Globalization;
using System.Net.Http;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestion;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazySuggestionOperation : ILazyOperation
    {
        private readonly SuggestionQuery _query;
        private readonly SuggestionOperation _operation;

        public LazySuggestionOperation(InMemoryDocumentSessionOperations session, SuggestionQuery query)
        {
            _query = query;
            _operation = new SuggestionOperation(session, query);
        }

        public GetRequest CreateRequest()
        {
            var uri = _query.GetRequestUri();
            var separator = uri.IndexOf('?');
            return new GetRequest
            {
                Url = uri.Substring(0, separator),
                Query = uri.Substring(separator, uri.Length - separator)
            };
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }

        public void HandleResponse(GetResponse response)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            var result = JsonDeserializationClient.SuggestQueryResult((BlittableJsonReaderObject)response.Result);
            _operation.SetResult(result);

            Result = _operation.Complete();
        }
    }
}
