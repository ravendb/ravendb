using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public sealed class GetSchemaValidationConfiguration : IMaintenanceOperation<SchemaValidationConfiguration>
{
    public RavenCommand<SchemaValidationConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetSchemaValidationCommand();
    }

    internal sealed class GetSchemaValidationCommand : RavenCommand<SchemaValidationConfiguration>
    {
        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/schema-validation/config";

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

            Result = JsonDeserializationClient.SchemaValidationConfiguration(response);
        }
    }
}
