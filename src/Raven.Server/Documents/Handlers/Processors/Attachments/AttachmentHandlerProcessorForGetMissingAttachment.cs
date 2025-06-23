using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal sealed class
        AttachmentHandlerProcessorForGetMissingAttachment : AbstractAttachmentHandlerProcessorForGetMissingAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForGetMissingAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async Task WriteMissingAttachmentsForCollection(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, string collection, long start, int pageSize, AttachmentType document, OperationCancelToken token)
        {
            using var tx = context.OpenReadTransaction();
            if (collection != Constants.Documents.Collections.AllDocumentsCollection)
            {
                // we are looking for attachments in a specific collection
                await WriteMissingAttachmentsInternal(RequestHandler.Database, RequestHandler.Database.DocumentsStorage.GetDocumentsFrom(
                    context,
                    collection,
                    etag: 0,
                    start,
                    pageSize), context, writer, document, token);
            }
            else
            {
                // we are looking for attachments in all collections
                await WriteMissingAttachmentsInternal(RequestHandler.Database, RequestHandler.Database.DocumentsStorage.GetDocumentsFrom(
                    context,
                    etag: 0,
                    start,
                    pageSize), context, writer, document, token);
            }
        }

        protected override async Task WriteMissingAttachmentsForRevisions(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, long start, int pageSize, AttachmentType revision, OperationCancelToken token)
        {
            using var tx = context.OpenReadTransaction();
            await WriteMissingAttachmentsInternal(RequestHandler.Database, RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, start, pageSize),
                context, writer, revision, token);
        }

        protected override void CheckCollectionAndThrowIfNeeded(string collection)
        {
            if (RequestHandler.Database.DocumentsStorage.GetCollection(collection, true) == null)
                throw new ArgumentException($"Query string '{collection}' was not recognized as valid collection name");
        }
    }
}
