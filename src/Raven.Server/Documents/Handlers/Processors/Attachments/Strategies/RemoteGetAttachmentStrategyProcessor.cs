using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal sealed class RemoteGetAttachmentStrategyProcessor : AbstractGetAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RemoteGetAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            IGetAttachmentStrategy.CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name, nameof(GetAttachmentOperation));
        }

        public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            tx.Dispose();
        }

        public override async Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, OperationCancelToken tcs)
        {
            using var downloader = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.GetDownloader(attachment, tcs);
            await using var stream = await RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.StreamForDownloadDestinationInternal(downloader, attachment.Base64Hash.ToString());
            await stream.CopyToAsync(RequestHandler.ResponseBodyStream(), tcs.Token);
        }
    }
}
