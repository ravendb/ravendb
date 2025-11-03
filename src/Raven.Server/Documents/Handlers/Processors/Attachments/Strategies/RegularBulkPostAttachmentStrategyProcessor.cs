using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Attachments;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal sealed class RegularBulkPostAttachmentStrategyProcessor : AbstractBulkPostAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RegularBulkPostAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            if (attachment.RemoteParameters.IsRemoteStorageAttachment())
            {
                throw new InvalidOperationException($"Cannot bulk get attachment '{name}' on document '{documentId}' because it is remote. Please use dedicated API.");
            }
        }

        public override Task<Stream> GetAttachmentStream(DirectFileDownloader downloader, Attachment attachment)
        {
            return Task.FromResult(attachment.Stream);
        }

        public override DirectFileDownloader GetAttachmentsDownloader(Attachment attachment, OperationCancelToken tcs)
        {
            return null;
        }
    }
}
