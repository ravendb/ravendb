using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Integrations.PostgreSQL
{
    public class ConfigurePostgreSqlOperation : IMaintenanceOperation<ConfigurePostgreSqlOperationResult>
    {
        private readonly PostgreSqlConfiguration _configuration;

        public ConfigurePostgreSqlOperation(PostgreSqlConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<ConfigurePostgreSqlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigurePostgreSqlCommand(_configuration);
        }

        private class ConfigurePostgreSqlCommand : RavenCommand<ConfigurePostgreSqlOperationResult>, IRaftCommand
        {
            private readonly PostgreSqlConfiguration _configuration;

            public ConfigurePostgreSqlCommand(PostgreSqlConfiguration configuration)
            {
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/integrations/postgresql/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false))
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigurePostgreSqlOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
