using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class IndexHasChangedOperation : IMaintenanceOperation<bool>
    {
        private readonly IndexDefinition _definition;

        public IndexHasChangedOperation(IndexDefinition definition)
        {
            _definition = definition ?? throw new ArgumentNullException(nameof(definition));
        }

        public RavenCommand<bool> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new IndexHasChangedCommand(conventions, context, _definition);
        }

        private class IndexHasChangedCommand : RavenCommand<bool>
        {
            private readonly BlittableJsonReaderObject _definition;

            public IndexHasChangedCommand(DocumentConventions conventions, JsonOperationContext context, IndexDefinition definition)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (definition == null)
                    throw new ArgumentNullException(nameof(definition));
                if (string.IsNullOrWhiteSpace(definition.Name))
                    throw new ArgumentNullException(nameof(definition.Name));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _definition = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(definition, context);
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/has-changed";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _definition).ConfigureAwait(false))
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
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
