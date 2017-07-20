using System;
using System.Net.Http;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestion;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazySuggestionOperation : ILazyOperation
    {
        private readonly SuggestionQuery _query;
        private readonly SuggestionOperation _operation;
        private readonly DocumentConventions _conventions;

        public LazySuggestionOperation(InMemoryDocumentSessionOperations session, SuggestionQuery query)
        {
            _query = query;
            _conventions = session.Conventions;
            _operation = new SuggestionOperation(session, query);
        }

        public GetRequest CreateRequest()
        {
            return new GetRequest
            {
                Url = "/queries?op=suggest",
                Method = HttpMethod.Post,
                Content = new SuggestionQueryContent(_conventions, _query)
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

        private class SuggestionQueryContent : GetRequest.IContent
        {
            private readonly DocumentConventions _conventions;
            private readonly SuggestionQuery _query;

            public SuggestionQueryContent(DocumentConventions conventions, SuggestionQuery query)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _query = query ?? throw new ArgumentNullException(nameof(query));
            }

            public void WriteContent(BlittableJsonTextWriter writer, JsonOperationContext context)
            {
                writer.WriteSuggestionQuery(_conventions, context, _query);
            }
        }
    }
}
