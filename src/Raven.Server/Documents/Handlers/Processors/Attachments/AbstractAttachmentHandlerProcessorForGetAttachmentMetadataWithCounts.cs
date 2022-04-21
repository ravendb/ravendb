using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal abstract class AbstractAttachmentHandlerProcessorForGetAttachmentMetadataWithCounts<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractAttachmentHandlerProcessorForGetAttachmentMetadataWithCounts([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) 
        : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask HandleAttachmentMetadataWithCountsAsync(string documentId);

    public override ValueTask ExecuteAsync()
    {
        var documentId = RequestHandler.GetStringQueryString("id");

        return HandleAttachmentMetadataWithCountsAsync(documentId);
    }
}
