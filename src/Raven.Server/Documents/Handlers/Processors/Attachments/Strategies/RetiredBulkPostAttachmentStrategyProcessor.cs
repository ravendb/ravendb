using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal class RetiredBulkPostAttachmentStrategyProcessor : AbstractBulkPostAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RetiredBulkPostAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            IGetAttachmentStrategy.CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name, "bulk");
        }

        public override async Task<Stream> GetAttachmentStream(DirectFileDownloader downloader, Attachment attachment)
        {
            return await RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.StreamForDownloadDestinationInternal(downloader,
                attachment.Base64Hash.ToString());
        }

        public override DirectFileDownloader GetAttachmentsDownloader(Attachment attachment, OperationCancelToken tcs)
        {
            return RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDownloader(attachment, tcs);
        }
    }
}
