using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class GetServerWideBackupConfigurationOperation : IServerOperation<ServerWideBackupConfiguration>
    {
        public RavenCommand<ServerWideBackupConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetServerWideBackupConfigurationCommand();
        }

        private class GetServerWideBackupConfigurationCommand : RavenCommand<ServerWideBackupConfiguration>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/backup";

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

                Result = JsonDeserializationClient.ServerWideBackupConfiguration(response);
            }
        }
    }
}
