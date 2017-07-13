using System;
using System.Net.Http;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyFacetsOperation : ILazyOperation
    {
        private readonly DocumentConventions _conventions;
        private readonly FacetQuery _query;

        public LazyFacetsOperation(DocumentConventions conventions, FacetQuery query)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _query = query ?? throw new ArgumentNullException(nameof(query));
        }

        public GetRequest CreateRequest()
        {
            return new GetRequest
            {
                Url = "/queries?op=facets",
                Method = HttpMethod.Post,
                Content = new FacetQueryContent(_conventions, _query)
            };
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }

        public void HandleResponse(GetResponse response)
        {
            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            Result = JsonDeserializationClient.FacetedQueryResult((BlittableJsonReaderObject)response.Result);
        }

        private class FacetQueryContent : GetRequest.IContent
        {
            private readonly DocumentConventions _conventions;
            private readonly FacetQuery _query;

            public FacetQueryContent(DocumentConventions conventions, FacetQuery query)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _query = query ?? throw new ArgumentNullException(nameof(query));
            }

            public void WriteContent(BlittableJsonTextWriter writer, JsonOperationContext context)
            {
                writer.WriteFacetQuery(_conventions, context, _query);
            }
        }
    }
}
