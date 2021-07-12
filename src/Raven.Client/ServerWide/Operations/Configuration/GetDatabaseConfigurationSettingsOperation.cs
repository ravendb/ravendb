using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class GetDatabaseConfigurationSettingsOperation : IServerOperation<DatabaseConfigurationSettings>
    {
        private readonly string _databaseName;

        public GetDatabaseConfigurationSettingsOperation(string databaseName)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        }

        public RavenCommand<DatabaseConfigurationSettings> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetDatabaseConfigurationSettingsCommand(_databaseName);
        }

        private class GetDatabaseConfigurationSettingsCommand : RavenCommand<DatabaseConfigurationSettings>
        {
            public override bool IsReadRequest => false;
            private readonly string _databaseName;

            public GetDatabaseConfigurationSettingsCommand(string databaseName)
            {
                _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/configuration/settings2";
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
                Result = JsonDeserializationClient.DatabaseConfigurationSettings(response);
            }
        }
    }
}
