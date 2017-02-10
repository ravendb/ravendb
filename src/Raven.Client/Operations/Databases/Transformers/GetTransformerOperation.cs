using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Operations;
using Sparrow.Json;

namespace Raven.NewClient.Client.Operations.Databases.Transformers
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