using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForGetRevisionsDebug : AbstractRevisionsHandlerProcessorForGetRevisionsDebug<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForGetRevisionsDebug([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            var (start, pageSize) = GetParameters();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Revisions");
                writer.WriteStartArray();

                var first = true;
                foreach (var revision in RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, start, pageSize))
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

        protected override async Task HandleRemoteNodeAsync(ProxyCommand<BlittableJsonReaderObject> command, OperationCancelToken token)
        {
            await RequestHandler.ExecuteRemoteAsync(command, token.Token);
        }
    }
}
