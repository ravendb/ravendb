using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class EnforceRevisionsConfigurationOperation : IOperation<OperationIdResult>
    {
        private readonly long? _operationId;
        private readonly bool _includeForceCreated;

        public EnforceRevisionsConfigurationOperation()
        {
        }

        public EnforceRevisionsConfigurationOperation(bool includeForceCreated)
        {
            _includeForceCreated = includeForceCreated;
        }

        internal EnforceRevisionsConfigurationOperation(long? operationId = null)
        {
            _operationId = operationId;
        }

        public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new EnforceRevisionsConfigurationCommand(_includeForceCreated, _operationId);
        }

        internal class EnforceRevisionsConfigurationCommand : RavenCommand<OperationIdResult>
        {
            private readonly bool _includeForceCreated;
            private readonly long? _operationId;

            public EnforceRevisionsConfigurationCommand(bool includeForceCreated, long? operationId = null)
            {
                _includeForceCreated = includeForceCreated;
                _operationId = operationId;
            }
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                var pathBuilder = new StringBuilder(node.Url)
                    .Append("/databases/")
                    .Append(node.Database)
                    .Append("/admin/revisions/config/enforce");

                pathBuilder.Append("?includeForceCreated=");
                pathBuilder.Append(_includeForceCreated);

                if (_operationId.HasValue)
                    pathBuilder.Append("&operationId=")
                        .Append(_operationId);

                url = pathBuilder.ToString();
                return request;
            }

            public override bool IsReadRequest => false;

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.OperationIdResult(response);
            }
        }
    }
}
