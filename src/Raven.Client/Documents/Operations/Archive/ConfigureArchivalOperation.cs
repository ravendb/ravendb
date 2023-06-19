using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Archival;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Archive
{
    public class ConfigureArchivalOperation : IMaintenanceOperation<ConfigureArchivalOperationResult>
    {
        private readonly ArchivalConfiguration _configuration;

        public ConfigureArchivalOperation(ArchivalConfiguration configuration)
        {
            _configuration = configuration;
        }

        public RavenCommand<ConfigureArchivalOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureArchivalCommand(conventions, _configuration);
        }

        private class ConfigureArchivalCommand : RavenCommand<ConfigureArchivalOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly ArchivalConfiguration _configuration;

            public ConfigureArchivalCommand(DocumentConventions conventions, ArchivalConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/archival/config";

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

                Result = JsonDeserializationClient.ConfigureArchivalOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
