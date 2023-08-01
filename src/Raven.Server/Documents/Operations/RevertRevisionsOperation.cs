using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Revisions;
using Sparrow.Json;

namespace Raven.Server.Documents.Operations
{
    public sealed class RevertRevisionsOperation : IMaintenanceOperation<OperationIdResult>
    {
        private readonly RevertRevisionsRequest _request;

        public RevertRevisionsOperation(DateTime time, long window)
        {
            _request = new RevertRevisionsRequest() {Time = time, WindowInSec = window};
        }

        public RevertRevisionsOperation(RevertRevisionsRequest request)
        {
            _request = request ?? throw new ArgumentNullException(nameof(request));
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RevertRevisionsCommand(conventions, _request);
        }

        public sealed class RevertRevisionsCommand : RavenCommand<OperationIdResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly RevertRevisionsRequest _request;
            private readonly long? _operationId;

            public RevertRevisionsCommand(DocumentConventions conventions, RevertRevisionsRequest request, long? operationId = null)
            {
                _conventions = conventions;
                _request = request;
                _operationId = operationId;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/revisions/revert";

                if (_operationId.HasValue)
                    url += $"?operationId={_operationId}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_request, ctx)).ConfigureAwait(false), _conventions)
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<OperationIdResult>(response);
            }
        }
    }
}
