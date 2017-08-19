using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class QueryStreamCommand : RavenCommand<StreamResult>
    {
        private readonly DocumentConventions _conventions;
        private readonly IndexQuery _indexQuery;

        public QueryStreamCommand(DocumentConventions conventions, IndexQuery query)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _indexQuery = query ?? throw new ArgumentNullException(nameof(query));
            ResponseType = RavenCommandResponseType.Empty;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteIndexQuery(_conventions, ctx, _indexQuery);
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
