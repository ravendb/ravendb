﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class PutServerWideExternalReplicationOperation : IServerOperation<ServerWideTaskResponse>
    {
        private readonly ServerWideExternalReplication _configuration;

        public PutServerWideExternalReplicationOperation(ServerWideExternalReplication configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<ServerWideTaskResponse> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideExternalReplicationCommand(context, _configuration);
        }

        private class PutServerWideExternalReplicationCommand : RavenCommand<ServerWideTaskResponse>, IRaftCommand
        {
            private readonly BlittableJsonReaderObject _configuration;

            public PutServerWideExternalReplicationCommand(JsonOperationContext context, ServerWideExternalReplication configuration)
            {
                if (configuration == null)
                    throw new ArgumentNullException(nameof(configuration));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _configuration = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/external-replication";

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
                Result = JsonDeserializationClient.PutServerWideConfigurationResponse(response);
            }
        }
    }
}
