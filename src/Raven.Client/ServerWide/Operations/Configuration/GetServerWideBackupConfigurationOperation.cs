using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class GetServerWideBackupConfigurationOperation : IServerOperation<ServerWideBackupConfiguration>
    {
        private readonly string _name;

        public GetServerWideBackupConfigurationOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand<ServerWideBackupConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetServerWideBackupConfigurationCommand(_name);
        }

        private class GetServerWideBackupConfigurationCommand : RavenCommand<ServerWideBackupConfiguration>
        {
            private readonly string _name;

            public GetServerWideBackupConfigurationCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/backup?name={Uri.EscapeDataString(_name)}";

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

                var results = JsonDeserializationClient.GetServerWideBackupConfigurationsResponse(response).Results;
                if (results.Length == 0)
                    return;

                if (results.Length > 1)
                    ThrowInvalidResponse();

                Result = results[0];
            }
        }
    }
}
