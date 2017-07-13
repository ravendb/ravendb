using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetMultiFacetsOperation : IOperation<FacetedQueryResult[]>
    {
        private readonly FacetQuery[] _queries;

        public GetMultiFacetsOperation(params FacetQuery[] queries)
        {
            if (queries == null || queries.Length == 0)
                throw new ArgumentNullException(nameof(queries));

            _queries = queries;
        }

        public RavenCommand<FacetedQueryResult[]> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetMultiFacetsCommand(conventions, context, cache, _queries);
        }

        private class GetMultiFacetsCommand : RavenCommand<FacetedQueryResult[]>
        {
            private readonly MultiGetCommand _command;

            public GetMultiFacetsCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache, FacetQuery[] queries)
            {
                var commands = new List<GetRequest>();
                foreach (var q in queries)
                    commands.Add(new LazyFacetsOperation(conventions, q).CreateRequest());

                _command = new MultiGetCommand(context, cache, commands);
                ResponseType = RavenCommandResponseType.Raw;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                return _command.CreateRequest(node, out url);
            }

            public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
            {
                _command.SetResponseRaw(response, stream, context);

                Result = new FacetedQueryResult[_command.Result.Count];
                for (var i = 0; i < _command.Result.Count; i++)
                {
                    var result = _command.Result[i];
                    Result[i] = JsonDeserializationClient.FacetedQueryResult((BlittableJsonReaderObject)result.Result);
                }
            }
        }
    }
}