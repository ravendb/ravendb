using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Documents
{
    public class DeleteByCollectionOperation : IOperation
    {
        private readonly string _collectionName;

        public DeleteByCollectionOperation(string collectionName)
        {
            if (collectionName == null)
                throw new ArgumentNullException(nameof(collectionName));

            _collectionName = collectionName;
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new DeleteByCollectionCommand(_collectionName);
        }

        private class DeleteByCollectionCommand : RavenCommand<OperationIdResult>
        {
            private readonly string _collectionName;

            public DeleteByCollectionCommand(string collectionName)
            {
                if (collectionName == null)
                    throw new ArgumentNullException(nameof(collectionName));

                _collectionName = collectionName;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/collections/docs?name={_collectionName}";
                IsReadRequest = false;

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }
        }
    }
}