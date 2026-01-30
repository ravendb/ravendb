using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Extensions;
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

        public override (Task<Stream> Stream, bool IsLocal) GetAttachmentStream(DocumentsOperationContext context, Dictionary<string, DirectFileDownloader> downloaders, Attachment attachment, OperationCancelToken token)
        {
            return (Task.FromResult(attachment.Stream), true);
        }
    }
}
