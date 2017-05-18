using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class PatchCollectionOperation : IOperation<OperationIdResult>
    {
        private readonly string _collectionName;
        private readonly PatchRequest _patch;

        public PatchCollectionOperation(string collectionName, PatchRequest patch)
        {
            _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
            _patch = patch ?? throw new ArgumentNullException(nameof(patch));
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PatchCollectionCommand(conventions, context, _collectionName, _patch);
        }

        private class PatchCollectionCommand : RavenCommand<OperationIdResult>
        {
            private readonly JsonOperationContext _context;
            private readonly string _collectionName;
            private readonly BlittableJsonReaderObject _patch;

            public PatchCollectionCommand(DocumentConventions conventions, JsonOperationContext context, string collectionName, PatchRequest patch)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (patch == null)
                    throw new ArgumentNullException(nameof(patch));

                _context = context ?? throw new ArgumentNullException(nameof(context));
                _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
                _patch = EntityToBlittable.ConvertEntityToBlittable(patch, conventions, _context);
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/collections/docs?name={_collectionName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _patch);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}