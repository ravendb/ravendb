using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

public interface IBulkPostAttachmentStrategy
{
    public string CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    public Task<Stream> GetAttachmentStream(DirectFileDownloader downloader, Attachment attachment, string collection);
    public DirectFileDownloader GetAttachmentsDownloader(OperationCancelToken tcs);
    public void WriteAttachmentDetails(AsyncBlittableJsonTextWriter writer, Attachment attachment, string documentId);
}
