using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal class RetiredAttachmentHandlerProcessorForBulkRetiredAttachment : AttachmentHandlerProcessorForBulkAttachment
    {
        public RetiredAttachmentHandlerProcessorForBulkRetiredAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override string CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            return RetiredAttachmentHandlerProcessorForGetRetiredAttachment.CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name);
        }

        public override async Task<Stream> GetAttachmentStream(DirectBackupDownloader downloader, Attachment attachment, string collection)
        {
            return await RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.StreamForDownloadDestinationInternal(downloader, attachment, collection);
        }
        public override DirectBackupDownloader GetAttachmentsDownloader(DocumentsOperationContext context, OperationCancelToken tcs)
        {
            return RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDownloader(context, tcs);
        }

        public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            tx.Dispose();
        }
    }
}
