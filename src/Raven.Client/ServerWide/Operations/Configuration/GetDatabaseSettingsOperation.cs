using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    /// <summary>
    /// Retrieves settings from the database record, including: Database topology, Ongoing task configurations, Index information, Revision settings, 
    /// and other relevant database configurations.
    /// </summary>
    public sealed class GetDatabaseSettingsOperation : IMaintenanceOperation<DatabaseSettings>
    {
        private readonly string _databaseName;

        /// <inheritdoc cref="GetDatabaseSettingsOperation"/>
        /// <param name="databaseName">The name of the database whose settings will be retrieved. Cannot be null.</param>
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
