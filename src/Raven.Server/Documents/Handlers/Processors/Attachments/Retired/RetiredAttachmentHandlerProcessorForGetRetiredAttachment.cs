using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal class RetiredAttachmentHandlerProcessorForGetRetiredAttachment : AttachmentHandlerProcessorForGetAttachment
    {
        public RetiredAttachmentHandlerProcessorForGetRetiredAttachment([NotNull] DatabaseRequestHandler requestHandler, bool isDocument) : base(requestHandler, isDocument)
        {

        }

        public override string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            return CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name);
        }

        public static string CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(DocumentsOperationContext context, DocumentDatabase database, Attachment attachment, string documentId, string name)
        {
            if (attachment.Flags.HasFlag(AttachmentFlags.Retired) == false)
            {
                throw new InvalidOperationException($"Cannot get retired attachment '{name}' on document '{documentId}' because it is not retired. Please use dedicated API.");
            }

            using var document = database.DocumentsStorage.Get(context, documentId, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
            if (document.TryGetCollection(out string collectionStr) == false)
            {
                throw new InvalidOperationException(
                    $"Cannot get retired attachment '{name}' on document '{documentId}' because it is doesn't have a collection. Should not happen a likely a bug !");
            }

            var config = database.ServerStore.Cluster.ReadRetireAttachmentsConfiguration(database.Name);

            if (config == null)
            {
                throw new InvalidOperationException(
                    $"Cannot get retired attachment '{name}' on document '{documentId}' because it is doesn't have a {nameof(RetireAttachmentsConfiguration)}.");
            }

            if (config.Disabled)
            {
                throw new InvalidOperationException(
                    $"Cannot get retired attachment '{name}' on document '{documentId}' because {nameof(RetireAttachmentsConfiguration)} is disabled.");
            }

            if (config.RetirePeriods.ContainsKey(collectionStr) == false)
            {
                throw new InvalidOperationException($"Cannot get retired attachment '{name}' on document '{documentId}' because doesn't have {nameof(RetireAttachmentsConfiguration)} for collection: '{collectionStr}'.");
            }

            return collectionStr;
        }

        public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            tx.Dispose();
        }
        protected override async Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, string collection, CancellationToken token)
        {
            var tcs = RequestHandler.CreateHttpRequestBoundOperationToken(token);
            using var downloader = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDownloader(context, tcs);
            await using var stream = await RequestHandler.Database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.StreamForDownloadDestinationInternal(downloader, attachment, collection);
            await WriteAttachmentToResponseStream(context, stream, token);
        }
    }
}
