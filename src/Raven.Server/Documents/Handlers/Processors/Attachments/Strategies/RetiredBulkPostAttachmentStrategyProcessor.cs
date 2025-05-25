using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal class RetiredBulkPostAttachmentStrategyProcessor : AbstractBulkPostAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RetiredBulkPostAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override string CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            return IGetAttachmentStrategy.CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name,
                "bulk");
        }

        public override async Task<Stream> GetAttachmentStream(DirectFileDownloader downloader, Attachment attachment, string collection)
        {
            return await RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.StreamForDownloadDestinationInternal(downloader,
                attachment, collection);
        }

        public override DirectFileDownloader GetAttachmentsDownloader(OperationCancelToken tcs)
        {
            return RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDownloader(tcs);
        }

        public override void WriteAttachmentDetails(AsyncBlittableJsonTextWriter writer, Attachment attachment, string documentId)
        {
            writer.WriteStartObject();
            WriteAttachmentDetailsInternal(writer, attachment, documentId);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.Flags));
            writer.WriteInteger((int)attachment.Flags);
        }
    }
}
