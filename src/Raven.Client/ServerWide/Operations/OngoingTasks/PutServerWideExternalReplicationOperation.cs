using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.OngoingTasks
{
    public class PutServerWideExternalReplicationOperation : IServerOperation<ServerWideExternalReplicationResponse>
    {
        private readonly ServerWideExternalReplication _configuration;

        public PutServerWideExternalReplicationOperation(ServerWideExternalReplication configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<ServerWideExternalReplicationResponse> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideExternalReplicationCommand(context, _configuration);
        }

        private class PutServerWideExternalReplicationCommand : RavenCommand<ServerWideExternalReplicationResponse>, IRaftCommand
        {
            private readonly BlittableJsonReaderObject _configuration;

            public PutServerWideExternalReplicationCommand(JsonOperationContext context, ServerWideExternalReplication configuration)
            {
                if (configuration == null)
                    throw new ArgumentNullException(nameof(configuration));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _configuration = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/external-replication";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _configuration).ConfigureAwait(false))
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.PutServerWideConfigurationResponse(response);
            }
        }
    }
}
