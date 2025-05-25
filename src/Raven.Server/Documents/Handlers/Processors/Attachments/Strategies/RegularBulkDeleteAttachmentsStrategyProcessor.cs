using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal class RegularBulkDeleteAttachmentsStrategyProcessor : AbstractBulkDeleteAttachmentsStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RegularBulkDeleteAttachmentsStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override MergedDeleteAttachmentsCommand MergedDeleteAttachmentsCommand(List<AttachmentRequest> attachmentRequests)
        {
            var cmd = new MergedDeleteAttachmentsCommand
            {
                Database = RequestHandler.Database,
                Deletes = attachmentRequests
            };
            return cmd;
        }

        public override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string docId, string name)
        {
            RegularDeleteAttachmentStrategyProcessor.CheckAttachmentFlagAndThrowIfNeededInternal(context, attachment, RequestHandler.Database, docId, name);
        }
    }
}
