using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions;
public sealed class AdoptOrphanedRevisionsOperation : IOperation<OperationIdResult>
{
    private readonly Parameters _parameters;

    public sealed class Parameters : ReveisionsOperationParameters
    {
        public string[] Collections { get; set; } = null;
    }

    public AdoptOrphanedRevisionsOperation()
        : this(new Parameters())
    {
    }

    public AdoptOrphanedRevisionsOperation(Parameters parameters)
    {
        _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
    }

    public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
    {
        return new AdoptOrphanedRevisionsCommand(_parameters, conventions);
    }

    internal sealed class AdoptOrphanedRevisionsCommand : RavenCommand<OperationIdResult>
    {
        private readonly Parameters _parameters;
        private readonly DocumentConventions _conventions;

        public AdoptOrphanedRevisionsCommand(Parameters parameters, DocumentConventions conventions)
        {
            _parameters = parameters;
            _conventions = conventions;
        }
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/admin/revisions/orphaned/adopt");

            url = pathBuilder.ToString();

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx);
                    await ctx.WriteAsync(stream, config).ConfigureAwait(false);
                }, _conventions)
            };

            return request;
        }

        public override bool IsReadRequest => false;

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.OperationIdResult(response);
        }
    }
}

