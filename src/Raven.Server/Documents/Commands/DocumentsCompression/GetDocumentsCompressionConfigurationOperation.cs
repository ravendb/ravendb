using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.DocumentsCompression
{
    public class GetDocumentsCompressionConfigurationOperation : IMaintenanceOperation<DocumentsCompressionConfiguration>
    {
        public RavenCommand<DocumentsCompressionConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetDocumentsCompressionConfigurationCommand();
        }

        internal class GetDocumentsCompressionConfigurationCommand : RavenCommand<DocumentsCompressionConfiguration>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/documents-compression/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationServer.DocumentsCompressionConfiguration(response);
            }
        }
    }
}
