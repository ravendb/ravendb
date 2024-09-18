using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using static Raven.Server.Utils.MetricCacher.Keys;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForBulkPostAttachment : AbstractAttachmentHandlerProcessorForBulkPostAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForBulkPostAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public virtual string CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                throw new InvalidOperationException($"Cannot bulk get attachment '{name}' on document '{documentId}' because it is retired. Please use dedicated API.");
            }

            return null;
        }
        public virtual Task<Stream> GetAttachmentStream(DirectBackupDownloader downloader, Attachment attachment, string collection)
        {
            return Task.FromResult(attachment.Stream);
        }

        public virtual DirectBackupDownloader GetAttachmentsDownloader(DocumentsOperationContext context, OperationCancelToken tcs)
        {
             return null;
        }

        public virtual void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            // noop
        }

        protected override async ValueTask GetAttachmentsAsync(DocumentsOperationContext context, BlittableJsonReaderArray attachments, AttachmentType type,
            OperationCancelToken tcs)
        {
            var tasks = new List<Task<Stream>>();
            var attachmentsStreams = new List<Stream>();

            using DocumentsTransaction tx = context.OpenReadTransaction();
            using (var downloader = GetAttachmentsDownloader(context, tcs))
            using (var stream = new MemoryStream())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(GetAttachmentsOperation.GetAttachmentsCommand.AttachmentsMetadata));
                    writer.WriteStartArray();
                    var first = true;

                    foreach (BlittableJsonReaderObject bjro in attachments)
                    {
                        if (bjro.TryGet(nameof(AttachmentRequest.DocumentId), out string id) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.DocumentId)}");
                        if (bjro.TryGet(nameof(AttachmentRequest.Name), out string name) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.Name)}");

                        var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, id, name, type, changeVector: null);
                        if (attachment == null)
                            continue;

                        var collection = CheckAttachmentFlagAndThrowIfNeeded(context, attachment, id, name);

                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        var attachmentStream = GetAttachmentStream(downloader, attachment, collection);
                        tasks.Add(attachmentStream);

                        WriteAttachmentDetails(writer, attachment, id);

                        await writer.MaybeFlushAsync(tcs.Token);
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();

                    await writer.MaybeFlushAsync(tcs.Token);
                }

                DisposeReadTransactionIfNeeded(tx);

                foreach (var t in tasks)
                {
                    await t;
                    attachmentsStreams.Add(t.Result);
                }

                stream.Position = 0;
                await stream.CopyToAsync(RequestHandler.ResponseBodyStream(), tcs.Token);
            }

            using (context.GetMemoryBuffer(out var buffer))
            {   
                var responseStream = RequestHandler.ResponseBodyStream();
                foreach (var stream in attachmentsStreams)
                {
                    await using (var tmpStream = stream)
                    {
                        var count = await tmpStream.ReadAsync(buffer.Memory.Memory, tcs.Token);
                        while (count > 0)
                        {
                            await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, count), tcs.Token);
                            count = await tmpStream.ReadAsync(buffer.Memory.Memory, tcs.Token);
                        }
                    }
                }
            }
        }
    }
}
