using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Attachments;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal class AttachmentHandlerProcessorForExists : AbstractAttachmentHandlerProcessorForExists<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AttachmentHandlerProcessorForExists([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override ValueTask<AttachmentExistsCommand.Response> GetResponseAsync(DocumentsOperationContext context, string hash)
    {
        using (context.OpenReadTransaction())
        using (Slice.From(context.Allocator, hash, out var hashSlice))
        {
            var count = AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice);

            return ValueTask.FromResult(new AttachmentExistsCommand.Response
            {
                Hash = hash,
                Count = count
            });
        }
    }
}
