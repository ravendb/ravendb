using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal sealed class RetiredGetAttachmentStrategyProcessor : AbstractGetAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RetiredGetAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            IGetAttachmentStrategy.CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name, "get");
        }

        public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            tx.Dispose();
        }

        public override async Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, OperationCancelToken tcs)
        {
            using var downloader = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDownloader(attachment, tcs);
            await using var stream = await RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.StreamForDownloadDestinationInternal(downloader, attachment.Base64Hash.ToString());
            await WriteAttachmentToResponseStream(context, stream, attachment, bytesRemaining: null, tcs.Token);
        }
    }
}
