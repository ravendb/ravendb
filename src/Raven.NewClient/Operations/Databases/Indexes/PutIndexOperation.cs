using System;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data.Indexes;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class PutIndexOperation : IAdminOperation<PutIndexResult>
    {
        private readonly string _indexName;
        private readonly IndexDefinition _indexDefinition;

        public PutIndexOperation(string indexName, IndexDefinition indexDefinition)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));
            if (indexDefinition == null)
                throw new ArgumentNullException(nameof(indexDefinition));

            _indexName = indexName;
            _indexDefinition = indexDefinition;
        }

        public RavenCommand<PutIndexResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new PutIndexCommand(conventions, context, _indexName, _indexDefinition);
        }

        private class PutIndexCommand : RavenCommand<PutIndexResult>
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
}