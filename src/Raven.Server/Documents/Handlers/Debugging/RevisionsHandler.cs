using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class RevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/get-revisions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisions()
        {
            var etag = GetLongQueryString("etag", false) ?? 0;
            var pageSize = GetPageSize();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Revisions");
                writer.WriteStartArray();

                var first = true;
                foreach (var revision in Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, etag, pageSize))
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(Document.Id));
                    writer.WriteString(revision.Id);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(Document.Etag));
                    writer.WriteInteger(revision.Etag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(Document.LastModified));
                    writer.WriteDateTime(revision.LastModified, true);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(Document.ChangeVector));
                    writer.WriteString(revision.ChangeVector);

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }
    }
}
