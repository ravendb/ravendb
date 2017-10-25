using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class CompactDatabaseOperation : IOperation<OperationIdResult>
    {
        private readonly CompactSettings _compactSettings;

        public CompactDatabaseOperation(CompactSettings compactSettings)
        {
            _compactSettings = compactSettings ?? throw new ArgumentNullException(nameof(compactSettings));
        }

        public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new CompactDatabaseCommand(conventions, context, _compactSettings);
        }

        private class CompactDatabaseCommand : RavenCommand<OperationIdResult>
        {
            private readonly BlittableJsonReaderObject _compactSettings;

            public CompactDatabaseCommand(DocumentConventions conventions, JsonOperationContext context, CompactSettings compactSettings)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (compactSettings == null)
                    throw new ArgumentNullException(nameof(compactSettings));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _compactSettings = EntityToBlittable.ConvertEntityToBlittable(compactSettings, conventions, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/compact";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, _compactSettings);
                    })

                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
