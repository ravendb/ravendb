using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Extensions;
using Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal sealed class AttachmentHandlerProcessorForBulkPostAttachment : AbstractAttachmentHandlerProcessorForBulkPostAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForBulkPostAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetAttachmentsAsync(DocumentsOperationContext context, BlittableJsonReaderArray attachments, AttachmentType type,
            OperationCancelToken operationCancelToken)
        {
            var tasks = new List<Task<Stream>>();
            bool canDisposeReadTransaction = true;
            var downloaders = new Dictionary<string, DirectFileDownloader>(StringComparer.OrdinalIgnoreCase);
            using DocumentsTransaction tx = context.OpenReadTransaction();
            var downloadToken = new OperationCancelToken(operationCancelToken.Token);
            try
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
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

                        IBulkPostAttachmentStrategy strategy;
                        if (attachment.RemoteParameters.IsRemoteStorageAttachment())
                        {
                            strategy = new RemoteBulkPostAttachmentStrategyProcessor(RequestHandler);
                        }
                        else
                        {
                            strategy = new RegularBulkPostAttachmentStrategyProcessor(RequestHandler);
                        }

                        strategy.CheckAttachmentFlagAndThrowIfNeeded(context, attachment, id, name);

                        if (first == false)
                            writer.WriteComma();
                        first = false;
                        (var getAttachmentStreamTask, var isLocal) = strategy.GetAttachmentStream(context, downloaders, attachment, downloadToken);
                        tasks.Add(getAttachmentStreamTask);
                        if (isLocal)
                        {
                            canDisposeReadTransaction = false;
                        }

                        strategy.WriteAttachmentDetails(writer, attachment, id);

                        await writer.MaybeFlushAsync(operationCancelToken.Token);
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();

                    await writer.MaybeFlushAsync(operationCancelToken.Token);
                }

                if (canDisposeReadTransaction)
                {
                    // all requested attachments were remote, we can dispose the read transaction
                    tx.Dispose();
                }

                foreach (var t in tasks)
                {
                    await using Stream stream = await t;

                    await stream.CopyToAsync(RequestHandler.ResponseBodyStream(), operationCancelToken.Token);
                }
            }
            catch
            {
                //cancel any ongoing downloads
                downloadToken.Cancel();

                // Handle exceptions
                foreach (var streamTask in tasks)
                {
                    try
                    {
                        if (streamTask.IsCompletedSuccessfully)
                        {
                            // dispose stream
                            await using var stream = await streamTask;
                        }
                    }
                    catch
                    {
                        // Ignore errors during cleanup
                    }
                }
            }
            finally
            {
                // dispose the s3/azure clients
                foreach (var kvp in downloaders)
                {
                    kvp.Value.Dispose();
                }
            }
        }
    }
}
