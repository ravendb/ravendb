using System;
using System.Net.Http;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    /// <summary>
    /// Represents a maintenance operation to configure the retired attachments feature in RavenDB.
    /// This operation allows setting up automatic retirement of attachments to external storage destinations.
    /// </summary>
    public sealed class ConfigureRetiredAttachmentsOperation : IMaintenanceOperation<ConfigureRetireAttachmentsOperationResult>
    {
        private readonly RetiredAttachmentsConfiguration _configuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfigureRetiredAttachmentsOperation"/> class.
        /// </summary>
        /// <param name="configuration">The configuration settings for retired attachments. Cannot be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the configuration validation fails.</exception>
        public ConfigureRetiredAttachmentsOperation(RetiredAttachmentsConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            configuration.AssertConfiguration();
            _configuration = configuration;
        }

        /// <summary>
        /// Gets the command that will be executed to configure retired attachments.
        /// </summary>
        /// <param name="conventions">The document conventions to use for the request.</param>
        /// <param name="ctx">The JSON operation context for serialization.</param>
        /// <returns>A <see cref="RavenCommand{T}"/> that configures retired attachments and returns the operation result.</returns>
        public RavenCommand<ConfigureRetireAttachmentsOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureAttachmentsRetireCommand(conventions, _configuration);
        }

        private sealed class ConfigureAttachmentsRetireCommand : RavenCommand<ConfigureRetireAttachmentsOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly RetiredAttachmentsConfiguration _configuration;

            public ConfigureAttachmentsRetireCommand(DocumentConventions conventions, RetiredAttachmentsConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/attachments/retire/config";

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

                Result = JsonDeserializationClient.ConfigureRetireAttachmentsOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
