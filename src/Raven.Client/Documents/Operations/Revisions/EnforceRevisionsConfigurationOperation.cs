using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    public sealed class EnforceRevisionsConfigurationOperation : IOperation<OperationIdResult>
    {
        private readonly Parameters _parameters;

        public sealed class Parameters
        {
            public bool IncludeForceCreated { get; set; } = false;
            public string[] Collections { get; set; } = null;
        }

        public EnforceRevisionsConfigurationOperation()
        {

        }

        public EnforceRevisionsConfigurationOperation(Parameters parameters)
        {
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new EnforceRevisionsConfigurationCommand(_parameters, conventions);
        }

        internal sealed class EnforceRevisionsConfigurationCommand : RavenCommand<OperationIdResult>
        {
            private readonly Parameters _parameters;
            private readonly DocumentConventions _conventions;

            public EnforceRevisionsConfigurationCommand(Parameters parameters, DocumentConventions conventions)
            {
                _parameters = parameters;
                _conventions = conventions;
            }
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url)
                    .Append("/databases/")
                    .Append(node.Database)
                    .Append("/admin/revisions/config/enforce");

                url = pathBuilder.ToString();

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx);
                            await ctx.WriteAsync(stream, config).ConfigureAwait(false);
                        }
                    }, _conventions)
                };
            }

            public override bool IsReadRequest => false;

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.OperationIdResult(response);
            }
        }
    }
}
