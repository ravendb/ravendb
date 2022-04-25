using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForGetDocSize : AbstractDocumentHandlerProcessorForGetDocSize<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForGetDocSize([NotNull] DocumentHandler requestHandler, [NotNull] JsonContextPoolBase<DocumentsOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected override async ValueTask HandleDocSize(string docId)
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var document = RequestHandler.Database.DocumentsStorage.GetDocumentMetrics(context, docId);
            if (document == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            var documentSizeDetails = new DocumentSizeDetails
            {
                DocId = docId,
                ActualSize = document.Value.ActualSize,
                HumaneActualSize = Sizes.Humane(document.Value.ActualSize),
                AllocatedSize = document.Value.AllocatedSize,
                HumaneAllocatedSize = Sizes.Humane(document.Value.AllocatedSize)
            };

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, documentSizeDetails.ToJson());
            }
        }
    }
}
