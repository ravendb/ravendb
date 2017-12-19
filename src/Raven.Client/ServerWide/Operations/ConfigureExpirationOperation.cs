using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Expiration;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ConfigureExpirationOperation : IMaintenanceOperation<ConfigureExpirationOperationResult>
    {
        private readonly ExpirationConfiguration _configuration;

        public ConfigureExpirationOperation(ExpirationConfiguration configuration)
        {
            _configuration = configuration;
        }

        public RavenCommand<ConfigureExpirationOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureExpirationCommand(_configuration);
        }

        public class ConfigureExpirationCommand : RavenCommand<ConfigureExpirationOperationResult>
        {
            private readonly ExpirationConfiguration _configuration;

            public ConfigureExpirationCommand(ExpirationConfiguration configuration)
            {
                _configuration = configuration;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/expiration/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_configuration, DocumentConventions.Default, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureExpirationOperationResult(response);
            }
        }
    }
}
