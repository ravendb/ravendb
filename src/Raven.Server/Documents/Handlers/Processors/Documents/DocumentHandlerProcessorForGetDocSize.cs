using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForGetDocSize : AbstractDocumentHandlerProcessorForGetDocSize<DocumentSizeDetails, DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForGetDocSize([NotNull] DocumentHandler requestHandler, [NotNull] JsonContextPoolBase<DocumentsOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected override void WriteDocSize(DocumentSizeDetails size, DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        context.Write(writer, size.ToJson());
    }

    protected override ValueTask<(HttpStatusCode StatusCode, DocumentSizeDetails SizeResult)> GetResultAndStatusCodeAsync(string docId, DocumentsOperationContext context)
    {
        using (context.OpenReadTransaction())
        {
            var document = RequestHandler.Database.DocumentsStorage.GetDocumentMetrics(context, docId);
            
            if (document == null)
            {
                return new ValueTask<(HttpStatusCode, DocumentSizeDetails)>((HttpStatusCode.NotFound, null));
            }

            var documentSizeDetails = new DocumentSizeDetails
            {
                DocId = docId,
                ActualSize = document.Value.ActualSize,
                HumaneActualSize = Sizes.Humane(document.Value.ActualSize),
                AllocatedSize = document.Value.AllocatedSize,
                HumaneAllocatedSize = Sizes.Humane(document.Value.AllocatedSize)
            };

            return new ValueTask<(HttpStatusCode, DocumentSizeDetails)>((HttpStatusCode.OK, documentSizeDetails));
        }
    }
}
