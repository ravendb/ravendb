using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class IndexHasChangedOperation : IAdminOperation<bool>
    {
        private readonly IndexDefinition _definition;

        public IndexHasChangedOperation(IndexDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            _definition = definition;
        }

        public RavenCommand<bool> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new IndexHasChangedCommand(conventions, context, _definition);
        }

        private class IndexHasChangedCommand : RavenCommand<bool>
        {
            private readonly JsonOperationContext _context;
            private readonly BlittableJsonReaderObject _definition;

            public IndexHasChangedCommand(DocumentConventions conventions, JsonOperationContext context, IndexDefinition definition)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (definition == null)
                    throw new ArgumentNullException(nameof(definition));
                if (string.IsNullOrWhiteSpace(definition.Name))
                    throw new ArgumentNullException(nameof(definition.Name));

                _context = context;
                _definition = EntityToBlittable.ConvertEntityToBlittable(definition, conventions, context);
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/has-changed";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _definition);
                    })
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                bool changed;
                if (response.TryGet("Changed", out changed) == false)
                    ThrowInvalidResponse();

                Result = changed;
            }
        }
    }
}