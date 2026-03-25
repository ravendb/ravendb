using System.Collections.Generic;
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

        public override (Task<Stream> Stream, bool IsLocal) GetAttachmentStream(DocumentsOperationContext context, Dictionary<string, DirectFileDownloader> downloaders, Attachment attachment, OperationCancelToken token)
        {
            AttachmentsStorage attachmentsStorage = RequestHandler.Database.DocumentsStorage.AttachmentsStorage;
            var stream = attachmentsStorage.GetAttachmentStream(context, attachment.Base64Hash);
            if (stream != null)
            {
                return (Task.FromResult(stream), true);
            }

            if (downloaders.TryGetValue(attachment.RemoteParameters.Identifier, out DirectFileDownloader downloader) == false)
            {
                downloader = attachmentsStorage.RemoteAttachmentsStorage.GetDownloader(attachment, token);
                downloaders[attachment.RemoteParameters.Identifier] = downloader;
            }

            return (attachmentsStorage.RemoteAttachmentsStorage.StreamForDownloadDestinationInternal(downloader,
                attachment.Base64Hash.ToString()), false);
        }
    }
}
