using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    public sealed class ConfigureRevisionsBinCleanerOperation : IMaintenanceOperation<ConfigureRevisionsBinCleanerOperationResult>
    {
        private readonly RevisionsBinConfiguration _configuration;

        /// <summary>
        /// Configure the revisions-bin cleaner which cleans the revisions bin automatically when enabled.
        /// </summary>
        /// <param name="configuration">The configuration for the revisions bin cleaner. This parameter cannot be null.</param>
        /// <exception cref="ArgumentNullException">Thrown if the <paramref name="configuration"/> is null.</exception>
        public ConfigureRevisionsBinCleanerOperation(RevisionsBinConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<ConfigureRevisionsBinCleanerOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureRevisionsBinCleanerCommand(conventions, _configuration);
        }

        private sealed class ConfigureRevisionsBinCleanerCommand : RavenCommand<ConfigureRevisionsBinCleanerOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly RevisionsBinConfiguration _configuration;

            public ConfigureRevisionsBinCleanerCommand(DocumentConventions conventions, RevisionsBinConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/revisions/bin-cleaner/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content =
                        new BlittableJsonContent(
                            async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx))
                                .ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureRevisionsBinCleanerOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
