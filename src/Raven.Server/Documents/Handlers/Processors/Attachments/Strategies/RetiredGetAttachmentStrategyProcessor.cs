using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal class RetiredGetAttachmentStrategyProcessor : AbstractGetAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RetiredGetAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {

        }

        public override string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            return IGetAttachmentStrategy.CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name, "get");
        }

        public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            tx.Dispose();
        }

        public override async Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, string collection, CancellationToken token)
        {
            var tcs = RequestHandler.CreateHttpRequestBoundOperationToken(token);
            using var downloader = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDownloader(attachment, tcs);
            await using var stream = await RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.StreamForDownloadDestinationInternal(downloader, attachment.Base64Hash.ToString());
            await WriteAttachmentToResponseStream(context, stream, attachment, bytesRemaining: null, token);
        }

    }
}
