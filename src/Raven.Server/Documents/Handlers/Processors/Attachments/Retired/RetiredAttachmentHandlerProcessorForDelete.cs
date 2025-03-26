using System;
using Elastic.Clients.Elasticsearch;
using JetBrains.Annotations;
using Microsoft.CodeAnalysis;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using static Raven.Server.Documents.AttachmentsStorage;
using static Raven.Server.Documents.Handlers.AttachmentHandler;
using static Raven.Server.Documents.Handlers.Processors.Attachments.Retired.RetiredAttachmentHandlerProcessorForBulkDelete;
using static Raven.Server.Utils.MetricCacher.Keys;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal sealed class RetiredAttachmentHandlerProcessorForDelete : AttachmentHandlerProcessorForDeleteAttachment
    {
        public RetiredAttachmentHandlerProcessorForDelete([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override MergedDeleteRetiredAttachmentCommand CreateMergedDeleteAttachmentCommand(string docId, string name, LazyStringValue changeVector)
        {
            var storageOnly = RequestHandler.GetBoolValueQueryString("storageOnly", required: false) ?? false;

            var cmd = new MergedDeleteRetiredAttachmentCommand
            {
                Database = RequestHandler.Database,
                ExpectedChangeVector = changeVector,
                DocumentId = docId,
                Name = name,
                DeleteState = storageOnly ? DeleteAttachmentState.DocumentRetiredAttachmentStorage : DeleteAttachmentState.DocumentRetiredAttachmentCloudStorage

            };
            return cmd;
        }

        protected override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, string docId, string name)
        {
            using (context.OpenReadTransaction())
            {
                CheckRetiredAttachmentFlagAndThrowIfNeededInternal(context, RequestHandler, docId, name);
            }
        }

        public static void CheckRetiredAttachmentFlagAndThrowIfNeededInternal(DocumentsOperationContext context, DatabaseRequestHandler requestHandler, string docId,
            string name)
        {

            Attachment attachment = requestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, docId, name, AttachmentType.Document, changeVector: null);
            if (attachment == null)
                return;

            if (attachment.Flags.HasFlag(AttachmentFlags.Retired) == false)
            {
                throw new InvalidOperationException($"Cannot delete retired attachment '{name}' on document '{docId}' because it is not retired. Please use dedicated Client API.");
            }

            var dbRecord = requestHandler.Database.ReadDatabaseRecord();

            if (dbRecord.RetiredAttachments == null)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetiredAttachmentsConfiguration)} is not configured.");
            }

            if (dbRecord.RetiredAttachments.Disabled)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetiredAttachmentsConfiguration)} is disabled.");
            }

            if (dbRecord.RetiredAttachments.HasUploader() == false)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetiredAttachmentsConfiguration)} does not have any uploader configured.");
            }
        }


        internal sealed class MergedDeleteRetiredAttachmentCommand : MergedDeleteAttachmentCommand
        {
            public DeleteAttachmentState DeleteState;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(DeleteState, context, DocumentId, Name, ExpectedChangeVector, collectionName: out _);
                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new MergedDeleteRetiredAttachmentCommandDto
                {
                    DocumentId = DocumentId,
                    Name = Name,
                    ExpectedChangeVector = ExpectedChangeVector,
                    DeleteState = DeleteState
                };
            }

            internal sealed class MergedDeleteRetiredAttachmentCommandDto : MergedDeleteAttachmentCommandDto
            {
                public DeleteAttachmentState DeleteState;
                public override MergedDeleteRetiredAttachmentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    return new MergedDeleteRetiredAttachmentCommand
                    {
                        DocumentId = DocumentId,
                        Name = Name,
                        ExpectedChangeVector = ExpectedChangeVector,
                        DeleteState = DeleteState,
                        Database = database
                    };
                }
            }
        }
    }
}
