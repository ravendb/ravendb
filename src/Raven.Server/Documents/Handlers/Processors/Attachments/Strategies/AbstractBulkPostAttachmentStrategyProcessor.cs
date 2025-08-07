using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

internal abstract class AbstractBulkPostAttachmentStrategyProcessor<TRequestHandler, TOperationContext> : AbstractAttachmentStrategyProcessor<TRequestHandler, TOperationContext>, IBulkPostAttachmentStrategy
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractBulkPostAttachmentStrategyProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public abstract string CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    public abstract Task<Stream> GetAttachmentStream(DirectFileDownloader downloader, Attachment attachment, string collection);
    public abstract DirectFileDownloader GetAttachmentsDownloader(Attachment attachment, OperationCancelToken tcs);

    public void WriteAttachmentDetails(AsyncBlittableJsonTextWriter writer, Attachment attachment, string documentId)
    {
        writer.WriteStartObject();
        writer.WritePropertyName(nameof(AttachmentDetails.Name));
        writer.WriteString(attachment.Name);
        writer.WriteComma();
        writer.WritePropertyName(nameof(AttachmentDetails.Hash));
        writer.WriteString(attachment.Base64Hash.ToString());
        writer.WriteComma();
        writer.WritePropertyName(nameof(AttachmentDetails.ContentType));
        writer.WriteString(attachment.ContentType);
        writer.WriteComma();
        writer.WritePropertyName(nameof(AttachmentDetails.Size));
        writer.WriteInteger(attachment.Size);
        writer.WriteComma();
        writer.WritePropertyName(nameof(AttachmentDetails.ChangeVector));
        writer.WriteString(attachment.ChangeVector);
        writer.WriteComma();
        writer.WritePropertyName(nameof(AttachmentDetails.DocumentId));
        writer.WriteString(documentId);
        writer.WriteComma();
        writer.WritePropertyName(nameof(AttachmentDetails.RetireParameters));
        if (attachment.RetireParameters == null)
        {
            writer.WriteNull();
        }
        else
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(RetireAttachmentParameters.Identifier));
            writer.WriteString(attachment.RetireParameters.Identifier);

            writer.WriteComma();
            writer.WritePropertyName(nameof(RetireAttachmentParameters.At));
            writer.WriteDateTime(attachment.RetireParameters.At, true);

            writer.WriteComma();
            writer.WritePropertyName(nameof(RetireAttachmentParameters.Flags));
            writer.WriteInteger((int)attachment.RetireParameters.Flags);

            writer.WriteEndObject();
        }

        writer.WriteEndObject();
    }
}
