using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal class AttachmentHandlerProcessorForGetAttachmentMetadataWithCounts : AbstractAttachmentHandlerProcessorForGetAttachmentMetadataWithCounts<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AttachmentHandlerProcessorForGetAttachmentMetadataWithCounts([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleAttachmentMetadataWithCountsAsync(string documentId)
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var array = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachmentsMetadataForDocumentWithCounts(context, documentId.ToLowerInvariant());
            
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetAttachmentMetadataWithCountsCommand.Response.Id));
                writer.WriteString(documentId);
                writer.WriteComma();
                writer.WriteArray(nameof(GetAttachmentMetadataWithCountsCommand.Response.Attachments), array.Select(x => x.ToJson()), context);
                writer.WriteEndObject();
            }
        }
    }
}
