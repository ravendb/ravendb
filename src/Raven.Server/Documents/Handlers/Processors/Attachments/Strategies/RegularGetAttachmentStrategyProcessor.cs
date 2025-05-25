using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{

    internal class RegularGetAttachmentStrategyProcessor : AbstractGetAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RegularGetAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            // noop
        }

        public override string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                throw new InvalidOperationException($"Cannot get attachment '{name}' on document '{documentId}' because it is retired. Please use dedicated API.");
            }

            return null;
        }

        public override async Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, string collection, CancellationToken token)
        {
            await using (var stream = attachment.Stream)
            {
                await WriteAttachmentToResponseStream(context, stream, token);
            }
        }

    }
}
