using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Transformers
{
    public class PutTransformerOperation : IAdminOperation<PutTransformerResult>
    {
        private readonly TransformerDefinition _transformerDefinition;

        public PutTransformerOperation(TransformerDefinition transformerDefinition)
        {
            _transformerDefinition = transformerDefinition ?? throw new ArgumentNullException(nameof(transformerDefinition));
        }

        public RavenCommand<PutTransformerResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutTransformerCommand(conventions, context, _transformerDefinition);
        }

        private class PutTransformerCommand : RavenCommand<PutTransformerResult>
        {
            private readonly JsonOperationContext _context;
            private readonly string _transformerName;
            private readonly BlittableJsonReaderObject _transformerDefinition;

            public PutTransformerCommand(DocumentConventions conventions, JsonOperationContext context, TransformerDefinition transformerDefinition)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (transformerDefinition == null)
                    throw new ArgumentNullException(nameof(transformerDefinition));

                _context = context ?? throw new ArgumentNullException(nameof(context));
                _transformerName = transformerDefinition.Name ?? throw new ArgumentNullException(nameof(transformerDefinition.Name));
                _transformerDefinition = EntityToBlittable.ConvertEntityToBlittable(transformerDefinition, conventions, _context);
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers?name=" + Uri.EscapeDataString(_transformerName);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _transformerDefinition);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.PutTransformerResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}