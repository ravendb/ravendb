using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal class DeleteRevisionsCommand : RavenCommand
    {
        private DeleteRevisionsRequest _request;

        public DeleteRevisionsCommand(DeleteRevisionsRequest request)
        {
            _request = request;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/revisions/delete";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_request, ctx)).ConfigureAwait(false),
                    DocumentConventions.Default)
            };
        }
    }

    public class DeleteRevisionsRequest
    {
        public long MaxDeletes { get; set; }
        public List<string> DocumentIds { get; set; }
        public List<string> RevisionsChangeVecotors { get; set; }
        internal bool ThrowAboutRevisionsChangeVecotors { get; set; } = true;
    }
}
