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
    internal class DeleteRevisionsManuallyCommand : RavenCommand<DeleteRevisionsManuallyOperation.Result>
    {
        private DeleteRevisionsRequest _request;

        public DeleteRevisionsManuallyCommand(DeleteRevisionsRequest request)
        {
            _request = request;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/revisions/";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
                Content = new BlittableJsonContent(async stream =>
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_request, ctx)).ConfigureAwait(false),
                    DocumentConventions.Default)
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.DeleteRevisionsManuallyResult(response);
        }
    }

    internal class DeleteRevisionsRequest
    {
        // Either!
        public List<string> RevisionsChangeVectors { get; set; }

        // Or!
        public string DocumentId { get; set; }
        public long MaxDeletes { get; set; } = 1024;
        public DateTime? After { get; set; } // start
        public DateTime? Before { get; set; } // end

        internal void Validate()
        {
            if (string.IsNullOrEmpty(DocumentId) && RevisionsChangeVectors.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(RevisionsChangeVectors)}, {nameof(DocumentId)}", "request 'DocumentIds' and 'RevisionsChangeVecotors' cannot be both null or empty.");
            }

            if (string.IsNullOrEmpty(DocumentId) == false && RevisionsChangeVectors.IsNullOrEmpty() == false)
            {
                throw new ArgumentException($"{nameof(RevisionsChangeVectors)}, {nameof(DocumentId)}", "The request contains values for both 'DocumentId' and 'RevisionsChangeVectors'. You can only provide one of them, the other must be null or empty.");
            }

            if (string.IsNullOrEmpty(DocumentId) == false)
            {
                ValidateDocumentId();
            }
        }

        internal void ValidateDocumentId()
        {
            if (MaxDeletes <= 0)
                throw new ArgumentException(nameof(MaxDeletes), "request 'MaxDeletes' have to be greater then 0.");

            if (After.HasValue && Before.HasValue && After >= Before)
                throw new ArgumentException($"{nameof(After)}, {nameof(Before)}", "'After' must be greater then 'Before'.");
        }
    }
}
