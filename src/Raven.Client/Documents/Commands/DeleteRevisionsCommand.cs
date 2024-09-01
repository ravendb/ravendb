using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal sealed class DeleteRevisionsCommand : RavenCommand<DeleteRevisionsOperation.Result>
    {
        private readonly DocumentConventions _conventions;
        private readonly DeleteRevisionsOperation.Parameters _parameters;

        public DeleteRevisionsCommand(DocumentConventions conventions, DeleteRevisionsOperation.Parameters parameters)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/revisions";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
                Content = new BlittableJsonContent(async stream =>
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx)).ConfigureAwait(false),
                    _conventions)
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.DeleteRevisionsResult(response);
        }
    }
}
