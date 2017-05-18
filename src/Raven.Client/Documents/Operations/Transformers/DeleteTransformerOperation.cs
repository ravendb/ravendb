using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Transformers
{
    public class DeleteTransformerOperation : IAdminOperation
    {
        private readonly string _transformerName;

        public DeleteTransformerOperation(string transformerName)
        {
            _transformerName = transformerName ?? throw new ArgumentNullException(nameof(transformerName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteTransformerCommand(_transformerName);
        }

        private class DeleteTransformerCommand : RavenCommand
        {
            private readonly string _transformerName;

            public DeleteTransformerCommand(string transformerName)
            {
                _transformerName = transformerName ?? throw new ArgumentNullException(nameof(transformerName));
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers?name={Uri.EscapeDataString(_transformerName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }
        }
    }
}