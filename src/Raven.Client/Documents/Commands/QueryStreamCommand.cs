using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class QueryStreamCommand : RavenCommand<StreamResult>
    {
        private readonly JsonOperationContext _context;
        private readonly BlittableJsonReaderObject _query;
        public readonly bool UsedTransformer;

        public QueryStreamCommand(DocumentConventions conventions, JsonOperationContext context, IndexQuery query)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            _context = context ?? throw new ArgumentNullException(nameof(context));
            _query = EntityToBlittable.ConvertEntityToBlittable(query, conventions, context);
            UsedTransformer = string.IsNullOrWhiteSpace(query.Transformer) == false;
            ResponseType = RavenCommandResponseType.Empty;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(_context, stream))
                    {
                        writer.WriteObject(_query);
                    }
                })
            };

            url = $"{node.Url}/databases/{node.Database}/streams/queries";
            return request;
        }

        public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            Result = new StreamResult
            {
                Response = response,
                Stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false)
            };

            return ResponseDisposeHandling.Manually;
        }

        public override bool IsReadRequest => true;
    }
}