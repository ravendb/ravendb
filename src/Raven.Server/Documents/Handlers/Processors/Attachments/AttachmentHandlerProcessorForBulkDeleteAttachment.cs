using System;
using System.Collections.Generic;
using System.Net.Mail;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForBulkDeleteAttachment : AbstractAttachmentHandlerProcessorForBulkDeleteAttachment<DatabaseRequestHandler,
        DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForBulkDeleteAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteAttachmentAsync(DocumentsOperationContext context, BlittableJsonReaderArray attachments, OperationCancelToken operationCancelToken)
        {
                        IBulkDeleteAttachmentsStrategy strategy;
            var attachmentRequests = new List<AttachmentRequest>();
            using (context.OpenReadTransaction())
            {
                foreach (BlittableJsonReaderObject bjro in attachments)
                {
                    using (bjro)
                    {
                        if (bjro.TryGet(nameof(AttachmentRequest.DocumentId), out string docId) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.DocumentId)}");
                        if (bjro.TryGet(nameof(AttachmentRequest.Name), out string name) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.Name)}");

                        //TODO: egor here I need to rework this logic, so same merged command will delete both regular and retired attachments, and if its retired then delete from storage only!
                        strategy = new RegularBulkDeleteAttachmentsStrategyProcessor(RequestHandler);
                            strategy = new RetiredBulkDeleteAttachmentsStrategyProcessor(RequestHandler);
                        //TODO: egor make this compile for now
                        //strategy.CheckAttachmentFlagAndThrowIfNeeded(context, docId, name);
                        attachmentRequests.Add(new AttachmentRequest(docId, name));
                    }
                }
            }

            if (attachmentRequests.Count == 0)
                return;
            //TODO: egor make this compile for now
            //MergedDeleteAttachmentsCommand cmd = strategy.MergedDeleteAttachmentsCommand(attachmentRequests);

            //await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }

    }

}
