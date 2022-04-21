using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Changes;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForGet : AbstractDocumentHandlerProcessorForGet<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForGet([NotNull] DocumentHandler requestHandler, [NotNull] JsonContextPoolBase<DocumentsOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected override async ValueTask GetDocumentsAsync(DocumentsOperationContext context, StringValues ids, bool metadataOnly)
    {
        using (context.OpenReadTransaction())
        {
            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.AddStringToHttpContext(ids.ToString(), TrafficWatchChangeType.Documents);

            if (ids.Count > 0)
                await RequestHandler.GetDocumentsByIdAsync(context, ids, metadataOnly);
            else
                await RequestHandler.GetDocumentsAsync(context, metadataOnly);
        }
    }
}
