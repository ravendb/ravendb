using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class PutServerWideClientConfigurationOperation : IServerOperation
    {
        private readonly ClientConfiguration _configuration;

        public PutServerWideClientConfigurationOperation(ClientConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideClientConfigurationCommand(conventions, context, _configuration);
        }

        private class PutServerWideClientConfigurationCommand : RavenCommand, IRaftCommand
        {
            private readonly BlittableJsonReaderObject _configuration;

            public PutServerWideClientConfigurationCommand(DocumentConventions conventions, JsonOperationContext context, ClientConfiguration configuration)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (configuration == null)
                    throw new ArgumentNullException(nameof(configuration));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _configuration = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/client";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _configuration).ConfigureAwait(false))
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
