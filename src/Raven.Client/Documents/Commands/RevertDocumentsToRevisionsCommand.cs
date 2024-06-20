using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal sealed class RevertDocumentsToRevisionsCommand : RavenCommand
    {
        private readonly Dictionary<string, string> _idToChangeVector;

        public RevertDocumentsToRevisionsCommand(Dictionary<string, string> idToChangeVector)
        {
            _idToChangeVector = idToChangeVector;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/revisions/revert/document";

            var request = new RevertDocumentsToRevisionsRequest{ IdToChangeVector = _idToChangeVector };

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(request, ctx)).ConfigureAwait(false),
                    DocumentConventions.Default)
            };
        }
    }

    internal class RevertDocumentsToRevisionsRequest
    {
        public Dictionary<string, string> IdToChangeVector { get; set; }
    }
}
