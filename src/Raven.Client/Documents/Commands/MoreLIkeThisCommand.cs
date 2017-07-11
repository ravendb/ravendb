using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class MoreLikeThisCommand : RavenCommand<MoreLikeThisQueryResult>
    {
        private readonly JsonOperationContext _context;
        private readonly BlittableJsonReaderObject _query;

        public MoreLikeThisCommand(DocumentConventions conventions, JsonOperationContext context, MoreLikeThisQuery query)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            _context = context ?? throw new ArgumentNullException(nameof(context));
            _query = EntityToBlittable.ConvertEntityToBlittable(query, conventions, context);
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
                    }
                )
            };

            url = $"{node.Url}/databases/{node.Database}/queries?op=morelikethis";
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.MoreLikeThisQueryResult(response);
        }

        public override bool IsReadRequest => true;
    }
}