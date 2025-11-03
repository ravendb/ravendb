using System;
using System.Net.Http;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Remote
{
    /// <summary>
    /// Represents a maintenance operation to configure the remote attachments feature in RavenDB.
    /// This operation allows setting up automatic upload of attachments to remote storage destinations.
    /// </summary>
    public sealed class ConfigureRemoteAttachmentsOperation : IMaintenanceOperation<ConfigureRemoteAttachmentsOperationResult>
    {
        private readonly RemoteAttachmentsConfiguration _configuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfigureRemoteAttachmentsOperation"/> class.
        /// </summary>
        /// <param name="configuration">The configuration settings for remote attachments. Cannot be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the configuration validation fails.</exception>
        public ConfigureRemoteAttachmentsOperation(RemoteAttachmentsConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            configuration.AssertConfiguration();
            _configuration = configuration;
        }

        /// <summary>
        /// Gets the command that will be executed to configure remote attachments.
        /// </summary>
        /// <param name="conventions">The document conventions to use for the request.</param>
        /// <param name="ctx">The JSON operation context for serialization.</param>
        /// <returns>A <see cref="RavenCommand{T}"/> that configures remote attachments and returns the operation result.</returns>
        public RavenCommand<ConfigureRemoteAttachmentsOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureAttachmentsRemoteCommand(conventions, _configuration);
        }

        private sealed class ConfigureAttachmentsRemoteCommand : RavenCommand<ConfigureRemoteAttachmentsOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly RemoteAttachmentsConfiguration _configuration;

            public ConfigureAttachmentsRemoteCommand(DocumentConventions conventions, RemoteAttachmentsConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/attachments/remote/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureRemoteAttachmentsOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
