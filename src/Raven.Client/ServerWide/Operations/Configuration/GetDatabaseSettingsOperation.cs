using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public sealed class GetDatabaseSettingsOperation : IMaintenanceOperation<DatabaseSettings>
    {
        private readonly string _databaseName;

        public GetDatabaseSettingsOperation(string databaseName)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        }

        public RavenCommand<DatabaseSettings> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetDatabaseSettingsCommand(_databaseName);
        }

        private sealed class GetDatabaseSettingsCommand : RavenCommand<DatabaseSettings>
        {
            public override bool IsReadRequest => false;
            private readonly string _databaseName;

            public GetDatabaseSettingsCommand(string databaseName)
            {
                _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/record";
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
                Result = JsonDeserializationClient.DatabaseSettings(response);
            }
        }
    }
}
