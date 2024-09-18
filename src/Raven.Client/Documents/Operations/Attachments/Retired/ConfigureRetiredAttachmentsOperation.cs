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
    public sealed class ConfigureRetiredAttachmentsOperation : IMaintenanceOperation<ConfigureRetireAttachmentsOperationResult>
    {
        private readonly RetiredAttachmentsConfiguration _configuration;

        public ConfigureRetiredAttachmentsOperation(RetiredAttachmentsConfiguration configuration)
        {
            configuration.AssertConfiguration();
            _configuration = configuration;
        }

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
                    Method = HttpMethod.Post,
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
