using System;
using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Documents
{
    public class DeleteCollectionOperation : IOperation<OperationIdResult>
    {
        private readonly string _collectionName;

        public DeleteCollectionOperation(string collectionName)
        {
            if (collectionName == null)
                throw new ArgumentNullException(nameof(collectionName));

            _collectionName = collectionName;
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new DeleteCollectionCommand(_collectionName);
        }

        private class DeleteCollectionCommand : RavenCommand<OperationIdResult>
        {
            private readonly string _collectionName;

            public DeleteCollectionCommand(string collectionName)
            {
                if (collectionName == null)
                    throw new ArgumentNullException(nameof(collectionName));

                _collectionName = collectionName;
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