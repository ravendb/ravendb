using System;
using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Indexing;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Transformers
{
    public class GetTransformerOperation : IAdminOperation<TransformerDefinition>
    {
        private readonly string _transformerName;

        public GetTransformerOperation(string transformerName)
        {
            if (transformerName == null)
                throw new ArgumentNullException(nameof(transformerName));

            _transformerName = transformerName;
        }

        public RavenCommand<TransformerDefinition> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetTransformerCommand(_transformerName);
        }

        private class GetTransformerCommand : RavenCommand<TransformerDefinition>
        {
            private readonly string _transformerName;

            public GetTransformerCommand(string transformerName)
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