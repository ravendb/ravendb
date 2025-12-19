using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

public interface IBulkPostAttachmentStrategy
{
    public void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    public (Task<Stream> Stream, bool IsLocal) GetAttachmentStream(DocumentsOperationContext context, Dictionary<string, DirectFileDownloader> downloaders, Attachment attachment, OperationCancelToken token);
    public void WriteAttachmentDetails(AsyncBlittableJsonTextWriter writer, Attachment attachment, string documentId);
}
