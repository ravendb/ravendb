using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Attachments;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal sealed class AttachmentHandlerProcessorForGetHashCount : AbstractAttachmentHandlerProcessorForGetHashCount<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AttachmentHandlerProcessorForGetHashCount([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask<GetAttachmentHashCountCommand.Response> GetResponseAsync(DocumentsOperationContext context, string hash)
    {
        using (context.OpenReadTransaction())
        using (Slice.From(context.Allocator, hash, out var hashSlice))
        {
            var count = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice);

            return ValueTask.FromResult(new GetAttachmentHashCountCommand.Response
            {
                Hash = hash,
                RegularCount = count.RegularHashes,
                RetiredCount = count.RetiredHashes,
                TotalCount = count.TotalHashes,

            });
        }
    }
}
