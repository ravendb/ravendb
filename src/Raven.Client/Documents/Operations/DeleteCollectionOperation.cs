using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class DeleteCollectionOperation : IOperation<OperationIdResult>
    {
        private readonly string _collectionName;

        public DeleteCollectionOperation(string collectionName)
        {
            _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteCollectionCommand(_collectionName);
        }

        private class DeleteCollectionCommand : RavenCommand<OperationIdResult>
        {
            private readonly string _collectionName;

            public DeleteCollectionCommand(string collectionName)
            {
                _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/collections/docs?name={_collectionName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
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