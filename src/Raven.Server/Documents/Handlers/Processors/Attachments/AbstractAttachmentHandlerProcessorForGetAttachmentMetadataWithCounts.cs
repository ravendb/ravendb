using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal abstract class AbstractAttachmentHandlerProcessorForGetAttachmentMetadataWithCounts<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAttachmentHandlerProcessorForGetAttachmentMetadataWithCounts([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask HandleAttachmentMetadataWithCountsAsync(string documentId);

    public override ValueTask ExecuteAsync()
    {
        var documentId = RequestHandler.GetStringQueryString("id");

        return HandleAttachmentMetadataWithCountsAsync(documentId);
    }
}
