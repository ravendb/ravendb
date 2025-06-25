using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal class RegularBulkPostAttachmentStrategyProcessor : AbstractBulkPostAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RegularBulkPostAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override string CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                throw new InvalidOperationException($"Cannot bulk get attachment '{name}' on document '{documentId}' because it is retired. Please use dedicated API.");
            }

            return null;
        }

        public override Task<Stream> GetAttachmentStream(DirectFileDownloader downloader, Attachment attachment, string collection)
        {
            return Task.FromResult(attachment.Stream);
        }

        public override DirectFileDownloader GetAttachmentsDownloader(OperationCancelToken tcs)
        {
            return null;
        }
    }
}
