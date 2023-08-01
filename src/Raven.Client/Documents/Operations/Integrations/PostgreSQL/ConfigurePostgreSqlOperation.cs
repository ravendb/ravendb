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
    public sealed class ConfigurePostgreSqlOperation : IMaintenanceOperation<ConfigurePostgreSqlOperationResult>
    {
        private readonly PostgreSqlConfiguration _configuration;

        public ConfigurePostgreSqlOperation(PostgreSqlConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<ConfigurePostgreSqlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigurePostgreSqlCommand(conventions, _configuration);
        }

        private sealed class ConfigurePostgreSqlCommand : RavenCommand<ConfigurePostgreSqlOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly PostgreSqlConfiguration _configuration;

            public ConfigurePostgreSqlCommand(DocumentConventions conventions, PostgreSqlConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/integrations/postgresql/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false), _conventions)
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
