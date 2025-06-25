using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{

    internal class RegularDeleteAttachmentStrategyProcessor : AbstractDeleteAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RegularDeleteAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override AttachmentHandler.MergedDeleteAttachmentCommand CreateMergedDeleteAttachmentCommand(string docId, string name, LazyStringValue changeVector)
        {
            var cmd = new AttachmentHandler.MergedDeleteAttachmentCommand
            {
                Database = RequestHandler.Database,
                ExpectedChangeVector = changeVector,
                DocumentId = docId,
                Name = name
            };
            return cmd;
        }

        public override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string docId, string name)
        {
            CheckAttachmentFlagAndThrowIfNeededInternal(context, attachment, RequestHandler.Database, docId, name);
        }

        public static void CheckAttachmentFlagAndThrowIfNeededInternal(DocumentsOperationContext context, Attachment attachment, DocumentDatabase database, string docId, string name)
        {
            if (attachment == null)
                return;

            //TODO: egor why this is needed
            using var _ = attachment.Stream;

            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because it is retired. Please use dedicated API.");
            }
        }
    }
}
