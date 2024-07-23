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
    internal class DeleteRevisionsManuallyCommand : RavenCommand
    {
        private DeleteRevisionsIntrenalRequest _request;

        public DeleteRevisionsManuallyCommand(DeleteRevisionsIntrenalRequest request)
        {
            _request = request;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/revisions/delete";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
                Content = new BlittableJsonContent(async stream =>
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_request, ctx)).ConfigureAwait(false),
                    DocumentConventions.Default)
            };
        }
    }

    public class DeleteRevisionsRequest
    {
        public long MaxDeletes { get; set; } = 1024;
        public List<string> DocumentIds { get; set; }
        public List<string> RevisionsChangeVecotors { get; set; }
    }

    internal class DeleteRevisionsIntrenalRequest : DeleteRevisionsRequest
    {
        public bool ThrowIfChangeVectorsNotFound { get; set; } = true;

        public DeleteRevisionsIntrenalRequest(){ }

        public DeleteRevisionsIntrenalRequest(DeleteRevisionsRequest other)
        {
            // copy constructor
            MaxDeletes = other.MaxDeletes;
            DocumentIds = other.DocumentIds;
            RevisionsChangeVecotors = other.RevisionsChangeVecotors;
        }
    }
}
