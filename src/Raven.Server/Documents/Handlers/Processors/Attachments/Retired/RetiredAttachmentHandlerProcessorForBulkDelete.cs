using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using static Raven.Server.Documents.AttachmentsStorage;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal class RetiredAttachmentHandlerProcessorForBulkDelete : AttachmentHandlerProcessorForBulkDeleteAttachment
    {
        public RetiredAttachmentHandlerProcessorForBulkDelete([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override MergedDeleteRetiredAttachmentsCommand MergedDeleteAttachmentsCommand(List<AttachmentRequest> attachmentRequests)
        {
            var storageOnly = RequestHandler.GetBoolValueQueryString("storageOnly", required: false) ?? false;
            var cmd = new MergedDeleteRetiredAttachmentsCommand
            {
                Database = RequestHandler.Database,
                Deletes = attachmentRequests,
                DeleteState = storageOnly ? DeleteAttachmentState.DocumentRetiredAttachmentStorage : DeleteAttachmentState.DocumentRetiredAttachmentCloudStorage
            };
            return cmd;
        }

        protected override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, string docId, string name)
        {
            RetiredAttachmentHandlerProcessorForDelete.CheckRetiredAttachmentFlagAndThrowIfNeededInternal(context, RequestHandler, docId, name);
        }

        internal sealed class MergedDeleteRetiredAttachmentsCommand : MergedDeleteAttachmentsCommand
        {
            public DeleteAttachmentState DeleteState;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                foreach (var delete in Deletes)
                {
                    Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(DeleteState, context, delete.DocumentId, delete.Name, null, collectionName: out _);
                }

                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction,
                MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new MergedDeleteRetiredAttachmentsCommandDto { Deletes = Deletes, DeleteState = DeleteState };
            }

            internal sealed class MergedDeleteRetiredAttachmentsCommandDto : MergedDeleteAttachmentsCommandDto
            {
                public DeleteAttachmentState DeleteState;

                public override MergedDeleteRetiredAttachmentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    return new MergedDeleteRetiredAttachmentsCommand { Deletes = Deletes, Database = database };
                }
            }
        }
    }
}
