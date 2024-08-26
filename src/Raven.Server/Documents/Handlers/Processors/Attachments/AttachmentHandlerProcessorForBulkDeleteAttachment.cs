using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
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

        protected override async ValueTask DeleteAttachmentAsync(DocumentsOperationContext context, BlittableJsonReaderArray attachments,
            OperationCancelToken operationCancelToken)
        {
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


                        CheckAttachmentFlagAndThrowIfNeeded(context, docId, name);
                        attachmentRequests.Add(new AttachmentRequest(docId, name));
                    }
                }
            }

            if (attachmentRequests.Count == 0)
                return;

            var cmd = new MergedDeleteAttachmentsCommand
            {
                Database = RequestHandler.Database,
                Deletes = attachmentRequests
            };

            await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }

        protected virtual void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, string docId, string name)
        {
            AttachmentHandlerProcessorForDeleteAttachment.CheckAttachmentFlagAndThrowIfNeededInternal(context, RequestHandler, docId, name);
        }
    }
    internal sealed class MergedDeleteAttachmentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        public List<AttachmentRequest> Deletes;
        public DocumentDatabase Database;

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            foreach (var delete in Deletes)
            {
                Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, delete.DocumentId, delete.Name, null, collectionName: out _);
            }

            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction,
            MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new MergedDeleteAttachmentsCommandDto { Deletes = Deletes };
        }
    }

    internal sealed class MergedDeleteAttachmentsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedDeleteAttachmentsCommand>
    {
        public List<AttachmentRequest> Deletes;

        public MergedDeleteAttachmentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new MergedDeleteAttachmentsCommand { Deletes = Deletes, Database = database };
        }
    }
}
