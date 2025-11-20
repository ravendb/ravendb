using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal sealed class RemoteBulkPostAttachmentStrategyProcessor : AbstractBulkPostAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RemoteBulkPostAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            IGetAttachmentStrategy.CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name, nameof(GetAttachmentsOperation));
        }

        public override (Task<Stream> Stream, bool IsLocal) GetAttachmentStream(DocumentsOperationContext context, DirectFileDownloader downloader, Attachment attachment)
        {
            var stream = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(context, attachment.Base64Hash);
            if (stream != null)
            {
                return (Task.FromResult(stream), true);
            }

            return (RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.StreamForDownloadDestinationInternal(downloader,
                attachment.Base64Hash.ToString()), false);
        }

        public override DirectFileDownloader GetAttachmentsDownloader(Attachment attachment, OperationCancelToken tcs)
        {
            return RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.GetDownloader(attachment, tcs);
        }
    }
}
