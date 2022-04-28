using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Attachments;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal class AttachmentHandlerProcessorForGetHashCount : AbstractAttachmentHandlerProcessorForGetHashCount<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AttachmentHandlerProcessorForGetHashCount([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask<GetAttachmentHashCountCommand.Response> GetResponseAsync(DocumentsOperationContext context, string hash)
    {
        using (context.OpenReadTransaction())
        using (Slice.From(context.Allocator, hash, out var hashSlice))
        {
            var count = AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice);

            return ValueTask.FromResult(new GetAttachmentHashCountCommand.Response
            {
                Hash = hash,
                Count = count
            });
        }
    }
}
