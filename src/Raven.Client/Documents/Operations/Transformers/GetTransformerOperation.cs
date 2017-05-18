using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Transformers;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Transformers
{
    public class GetTransformerOperation : IAdminOperation<TransformerDefinition>
    {
        private readonly string _transformerName;

        public GetTransformerOperation(string transformerName)
        {
            _transformerName = transformerName ?? throw new ArgumentNullException(nameof(transformerName));
        }

        public RavenCommand<TransformerDefinition> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetTransformerCommand(_transformerName);
        }

        private class GetTransformerCommand : RavenCommand<TransformerDefinition>
        {
            private readonly string _transformerName;

            public GetTransformerCommand(string transformerName)
            {
                _transformerName = transformerName ?? throw new ArgumentNullException(nameof(transformerName));
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers?name={Uri.EscapeDataString(_transformerName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.GetTransformersResponse(response).Results[0];
            }

            public override bool IsReadRequest => true;
        }
    }
}