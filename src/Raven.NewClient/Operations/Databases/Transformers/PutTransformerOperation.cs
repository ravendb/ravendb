using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data.Transformers;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Operations;
using Sparrow.Json;

namespace Raven.NewClient.Client.Operations.Databases.Transformers
{
    public class PutTransformerOperation : IAdminOperation<PutTransformerResult>
    {
        private readonly string _transformerName;
        private readonly TransformerDefinition _transformerDefinition;

        public PutTransformerOperation(string transformerName, TransformerDefinition transformerDefinition)
        {
            if (transformerName == null)
                throw new ArgumentNullException(nameof(transformerName));
            if (transformerDefinition == null)
                throw new ArgumentNullException(nameof(transformerDefinition));

            _transformerName = transformerName;
            _transformerDefinition = transformerDefinition;
        }

        public RavenCommand<PutTransformerResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new PutTransformerCommand(conventions, context, _transformerName, _transformerDefinition);
        }

        private class PutTransformerCommand : RavenCommand<PutTransformerResult>
        {
            private readonly JsonOperationContext _context;
            private readonly string _transformerName;
            private readonly BlittableJsonReaderObject _transformerDefinition;

            public PutTransformerCommand(DocumentConvention conventions, JsonOperationContext context, string transformerName, TransformerDefinition transformerDefinition)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (transformerName == null)
                    throw new ArgumentNullException(nameof(transformerName));
                if (transformerDefinition == null)
                    throw new ArgumentNullException(nameof(transformerDefinition));

                _context = context;
                _transformerName = transformerName;
                _transformerDefinition = new EntityToBlittable(null).ConvertEntityToBlittable(transformerDefinition, conventions, _context);
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers?name=" + Uri.EscapeUriString(_transformerName);

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