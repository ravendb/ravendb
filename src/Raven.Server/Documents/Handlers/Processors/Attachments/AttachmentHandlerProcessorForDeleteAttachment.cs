using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForDeleteAttachment : AbstractAttachmentHandlerProcessorForDeleteAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForDeleteAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteAttachmentAsync(DocumentsOperationContext context, string docId, string name, LazyStringValue changeVector)
        {
            var cmd = new AttachmentHandler.MergedDeleteAttachmentCommand
            {
                Database = RequestHandler.Database,
                ExpectedChangeVector = changeVector,
                DocumentId = docId,
                Name = name
            };
            await RequestHandler.Database.TxMerger.Enqueue(cmd);

            //TODO: egor I think we can simply delete the attachment now, and handle the storageOnly for retired attachment in the storage it self :)
            //Attachment attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, docId, name, AttachmentType.Document, changeVector: null);

            //IDeleteAttachmentStrategy strategy;
            //if (attachment == null)
            //{
            //    strategy = new RegularDeleteAttachmentStrategyProcessor(RequestHandler);
            //}
            //else if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            //{
            //    strategy = new RetiredDeleteAttachmentStrategyProcessor(RequestHandler);
            //}
            //else
            //{
            //    strategy = new RegularDeleteAttachmentStrategyProcessor(RequestHandler);
            //}

            //strategy.CheckAttachmentFlagAndThrowIfNeeded(context, attachment, docId, name);

            //var cmd = strategy.CreateMergedDeleteAttachmentCommand(docId, name, changeVector);
            //await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }
    }
}
