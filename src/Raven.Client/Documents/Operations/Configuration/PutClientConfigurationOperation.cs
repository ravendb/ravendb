using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Configuration
{
    /// <summary>
    /// Sets or modifies the client configuration on the server using the PutClientConfigurationOperation.
    /// The client configuration consists of various options that control client-server communication.
    /// 
    /// <para>The initial configuration is set when creating the Document Store, but it can be dynamically modified by a database administrator using this operation.</para>
    /// 
    /// <para><strong>Note:</strong> The client will update its configuration the next time it sends a request to the database.</para>
    /// </summary>
    public sealed class PutClientConfigurationOperation : IMaintenanceOperation
    {
        private readonly ClientConfiguration _configuration;

        /// <inheritdoc cref="PutClientConfigurationOperation" />
        /// <param name="configuration">The ClientConfiguration object containing the configuration settings to be applied on the server.</param>
        public PutClientConfigurationOperation(ClientConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutClientConfigurationCommand(conventions, context, _configuration);
        }

        private sealed class PutClientConfigurationCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _configuration;

            public PutClientConfigurationCommand(DocumentConventions conventions, JsonOperationContext context, ClientConfiguration configuration)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (configuration == null)
                    throw new ArgumentNullException(nameof(configuration));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _conventions = conventions;

                _configuration = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/configuration/client";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _configuration).ConfigureAwait(false), _conventions)
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
