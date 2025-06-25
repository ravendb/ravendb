using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal abstract class AbstractAttachmentHandlerProcessorForGetMissingAttachment<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {

        protected AbstractAttachmentHandlerProcessorForGetMissingAttachment([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var types = new List<AttachmentType>(2);
            var typeString = RequestHandler.GetStringQueryString("type", required: false);
            var collection = RequestHandler.GetStringQueryString("collection", required: true);
            if (string.IsNullOrEmpty(typeString) == false)
            {
                if (Enum.TryParse(typeString, true, out AttachmentType type) == false)
                {
                    throw new ArgumentException($"Query string '{typeString}' was not recognized as valid type");
                }

                types.Add(type);
            }
            else
            {
                types.Add(AttachmentType.Document);
                types.Add(AttachmentType.Revision);
            }

            if (string.IsNullOrEmpty(collection) == false)
            {
                if (collection.Equals(Constants.Documents.Collections.AllDocumentsCollection, StringComparison.OrdinalIgnoreCase) == false)
                {
                    CheckCollectionAndThrowIfNeeded(collection);
                }
            }

            var start = RequestHandler.GetLongQueryString("start", required: false) ?? 0;
            var pageSize = RequestHandler.GetPageSize();

            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            using (ContextPool.AllocateOperationContext(out TOperationContext context)) 
            //using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                if (types.Contains(AttachmentType.Revision))
                {
                    writer.WritePropertyName("Revisions");
                    writer.WriteStartObject();
                    await WriteMissingAttachmentsForRevisions(context, writer, start, pageSize, AttachmentType.Revision, token);

                    writer.WriteEndObject();
                }

                if (types.Contains(AttachmentType.Document))
                {
                    if (types.Contains(AttachmentType.Revision))
                        writer.WriteComma();

                    writer.WritePropertyName("Documents");
                    writer.WriteStartObject();

                    await WriteMissingAttachmentsForCollection(context, writer, collection, start, pageSize, AttachmentType.Document, token);

                    writer.WriteEndObject();
                }

                writer.WriteEndObject();
                await writer.FlushAsync(token.Token);
            }
        }

        protected abstract Task WriteMissingAttachmentsForCollection(TOperationContext context, AsyncBlittableJsonTextWriter writer, string collection, long start, int pageSize, AttachmentType document, OperationCancelToken token);
        protected abstract Task WriteMissingAttachmentsForRevisions(TOperationContext context, AsyncBlittableJsonTextWriter writer, long start, int pageSize, AttachmentType revision, OperationCancelToken token);
        protected abstract void CheckCollectionAndThrowIfNeeded(string collection);

        protected static async Task WriteMissingAttachmentsInternal(DocumentDatabase database, IEnumerable<Document> results, DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, AttachmentType attachmentType, OperationCancelToken token)
        {
            bool firstResult = true;
            foreach (var result in results)
            {
                using (result)
                {
                    token.ThrowIfCancellationRequested();
                    if (result.Flags.Contain(DocumentFlags.HasAttachments))
                    {
                        var currentAttachmentsInMetadata = AttachmentsStorage.GetAttachmentsFromDocumentMetadata(result.Data).Select(x => JsonDeserializationServer.MissingAttachmentInfo(x)).ToList();
                        var currentAttachmentsInTable = database.DocumentsStorage.AttachmentsStorage.GetAttachmentsForDocument(context, attachmentType, result.Id, result.ChangeVector).ToList();

                        var missing = new List<AttachmentHandler.MissingAttachmentInfo>();
                        foreach (var attachment in currentAttachmentsInMetadata)
                        {
                            token.ThrowIfCancellationRequested();

                            // Find missing attachments by name/hash
                            var exists = currentAttachmentsInTable.FirstOrDefault(x => x.Name == attachment.Name && x.Base64Hash.ToString() == attachment.Hash);
                            if (exists == null)
                            {
                                attachment.MissingSource = AttachmentHandler.MissingSource.Table;
                                attachment.AttachmentType = attachmentType;
                                missing.Add(attachment);
                            }

                            // Also check for missing hashes in storage
                            using (Slice.From(context.Allocator, attachment.Hash, out var hashSlice))
                            {
                                var count = database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice);
                                if (count == 0)
                                {
                                    attachment.MissingSource = AttachmentHandler.MissingSource.Hash;
                                    attachment.AttachmentType = attachmentType;

                                    missing.Add(attachment);
                                }
                            }
                        }

                        if (missing.Count > 0)
                        {
                            if (!firstResult)
                                writer.WriteComma();
                            firstResult = false;

                            writer.WritePropertyName(result.Id);
                            writer.WriteStartArray();
                            bool firstAttachment = true;
                            foreach (var att in missing)
                            {
                                token.ThrowIfCancellationRequested();
                                if (!firstAttachment)
                                    writer.WriteComma();
                                firstAttachment = false;

                                writer.WriteStartObject();
                                writer.WritePropertyName(nameof(AttachmentHandler.MissingAttachmentInfo.Name));
                                writer.WriteString(att.Name);
                                writer.WriteComma();
                                writer.WritePropertyName(nameof(AttachmentHandler.MissingAttachmentInfo.Hash));
                                writer.WriteString(att.Hash);
                                writer.WriteComma();
                                writer.WritePropertyName(nameof(AttachmentHandler.MissingAttachmentInfo.MissingSource));
                                writer.WriteString(att.MissingSource.ToString());
                                writer.WriteComma();
                                writer.WritePropertyName(nameof(AttachmentHandler.MissingAttachmentInfo.AttachmentType));
                                writer.WriteString(att.AttachmentType.ToString());
                                writer.WriteEndObject();
                            }
                            writer.WriteEndArray();
                            await writer.MaybeFlushAsync(token.Token);
                        }
                    }
                }
            }
        }
    }
}
