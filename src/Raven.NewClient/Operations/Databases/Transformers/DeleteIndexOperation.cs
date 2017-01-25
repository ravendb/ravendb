using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Operations;
using Sparrow.Json;

namespace Raven.NewClient.Client.Operations.Databases.Transformers
{
    public class DeleteTransformerOperation : IAdminOperation
    {
        private readonly string _transformerName;

        public DeleteTransformerOperation(string transformerName)
        {
            if (transformerName == null)
                throw new ArgumentNullException(nameof(transformerName));

            _transformerName = transformerName;
        }

        public RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new DeleteTransformerCommand(_transformerName);
        }

        private class DeleteTransformerCommand : RavenCommand<object>
        {
            private readonly string _transformerName;

            public DeleteTransformerCommand(string transformerName)
            {
                if (transformerName == null)
                    throw new ArgumentNullException(nameof(transformerName));

                _transformerName = transformerName;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers?name={Uri.EscapeUriString(_transformerName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
            }

            public override bool IsReadRequest => false;
        }
    }
}