using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal sealed class RegularGetAttachmentStrategyProcessor : AbstractGetAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RegularGetAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            // noop
        }

        public override void CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            if (attachment.RetireParameters.IsRetiredAttachment())
            {
                throw new InvalidOperationException($"Cannot get attachment '{name}' on document '{documentId}' because it is retired. Please use dedicated API.");
            }
        }

        public override async Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, OperationCancelToken tcs)
        {
            var (sendBody, start, bytesRemaining) = RangeHelper.SetRangeHeaders(HttpContext, attachment.Size);
            if (!sendBody)
                return;

            await using (var stream = attachment.Stream)
            {
                if (start > 0)
                {
                    stream.Seek(start, SeekOrigin.Begin);
                }

                await WriteAttachmentToResponseStream(context, stream, attachment, bytesRemaining, tcs.Token);
            }
        }
    }
}
