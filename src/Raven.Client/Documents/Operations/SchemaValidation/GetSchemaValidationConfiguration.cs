using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public sealed class GetSchemaValidationConfiguration : IMaintenanceOperation<GetSchemaValidationConfiguration.Result>
{
    public RavenCommand<Result> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetSchemaValidationCommand();
    }

    internal sealed class GetSchemaValidationCommand : RavenCommand<Result>
    {
        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/configuration/client";

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

            Result = JsonDeserializationClient.GetSchemaValidationConfiguration(response);
        }
    }

    public sealed class Result
    {
        public SchemaValidationConfiguration Configuration { get; set; }
    }
}
