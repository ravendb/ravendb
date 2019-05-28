using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class PutServerWideBackupConfigurationOperation : IServerOperation<PutServerWideBackupConfigurationResponse>
    {
        private readonly ServerWideBackupConfiguration _configuration;

        public PutServerWideBackupConfigurationOperation(ServerWideBackupConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<PutServerWideBackupConfigurationResponse> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideClientConfigurationCommand(context, _configuration);
        }

        private class PutServerWideClientConfigurationCommand : RavenCommand<PutServerWideBackupConfigurationResponse>
        {
            private readonly BlittableJsonReaderObject _configuration;

            public PutServerWideClientConfigurationCommand(JsonOperationContext context, ServerWideBackupConfiguration configuration)
            {
                if (configuration == null)
                    throw new ArgumentNullException(nameof(configuration));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _configuration = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/backup";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, _configuration);
                    })
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.PutServerWideBackupConfigurationResponse(response);
            }
        }
    }
}
