using System;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class PutIndexCommand : RavenCommand<PutIndexResult>
    {
        private readonly JsonOperationContext _context;
        private readonly string _indexName;
        private readonly BlittableJsonReaderObject _indexDefinition;

        public PutIndexCommand(DocumentConvention conventions, JsonOperationContext context, string indexName, IndexDefinition indexDefinition)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));
            if (indexDefinition == null)
                throw new ArgumentNullException(nameof(indexDefinition));

            _context = context;
            _indexName = indexName;
            _indexDefinition = new EntityToBlittable(null).ConvertEntityToBlittable(indexDefinition, conventions, _context);
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/indexes?name=" + Uri.EscapeUriString(_indexName);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    _context.Write(stream, _indexDefinition);
                })
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.PutIndexResult(response);
        }

        public override bool IsReadRequest => false;
    }
}