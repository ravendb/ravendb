// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public sealed class AttachmentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/attachments", "HEAD", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Head()
        {
            using (var processor = new AttachmentHandlerProcessorForHeadAttachment(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (var processor = new AttachmentHandlerProcessorForGetAttachment(this, isDocument: true))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetPost()
        {
            using (var processor = new AttachmentHandlerProcessorForGetAttachment(this, isDocument: false))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments/bulk", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetAttachments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var request = await context.ReadForDiskAsync(RequestBodyStream(), "GetAttachments");

                if (request.TryGet(nameof(AttachmentType), out string typeString) == false || Enum.TryParse(typeString, out AttachmentType type) == false)
                    throw new ArgumentException($"The '{nameof(AttachmentType)}' field in the body request is mandatory");

                if (request.TryGet(nameof(GetAttachmentsOperation.GetAttachmentsCommand.Attachments), out BlittableJsonReaderArray attachments) == false)
                    throw new ArgumentException($"The '{nameof(GetAttachmentsOperation.GetAttachmentsCommand.Attachments)}' field in the body request is mandatory");

                var attachmentsStreams = new List<Stream>();
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

                        var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, id, name, type, changeVector: null);
                        if (attachment == null)
                            continue;

                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        attachment.Size = attachment.Stream.Length;
                        attachmentsStreams.Add(attachment.Stream);
                        WriteAttachmentDetails(writer, attachment, id);

                        await writer.MaybeFlushAsync(Database.DatabaseShutdown);
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();

                    await writer.FlushAsync(Database.DatabaseShutdown);
                }

                using (context.GetMemoryBuffer(out var buffer))
                {
                    var responseStream = ResponseBodyStream();
                    foreach (var stream in attachmentsStreams)
                    {
                        await using (var tmpStream = stream)
                        {
                            var count = await tmpStream.ReadAsync(buffer.Memory.Memory);
                            while (count > 0)
                            {
                                await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, count), Database.DatabaseShutdown);
                                count = await tmpStream.ReadAsync(buffer.Memory.Memory);
                            }
                        }
                    }
                }
            }
        }

        private static void WriteAttachmentDetails(AsyncBlittableJsonTextWriter writer, Attachment attachment, string documentId)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(AttachmentDetails.Name));
            writer.WriteString(attachment.Name);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.Hash));
            writer.WriteString(attachment.Base64Hash.ToString());
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.ContentType));
            writer.WriteString(attachment.ContentType);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.Size));
            writer.WriteInteger(attachment.Size);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.ChangeVector));
            writer.WriteString(attachment.ChangeVector);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.DocumentId));
            writer.WriteString(documentId);
            writer.WriteEndObject();
        }

        [RavenAction("/databases/*/debug/attachments/hash", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetHashCount()
        {
            using (var processor = new AttachmentHandlerProcessorForGetHashCount(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/attachments/metadata", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetAttachmentMetadataWithCounts()
        {
            using (var processor = new AttachmentHandlerProcessorForGetAttachmentMetadataWithCounts(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Put()
        {
            using (var processor = new AttachmentHandlerProcessorForPutAttachment(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Delete()
        {
            using (var processor = new AttachmentHandlerProcessorForDeleteAttachment(this))
            {
                await processor.ExecuteAsync();
            }
        }

        public sealed class MergedPutAttachmentCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            public string DocumentId;
            public string Name;
            public LazyStringValue ExpectedChangeVector;
            public DocumentDatabase Database;
            public AttachmentDetails Result;
            public string ContentType;
            public Stream Stream;
            public string Hash;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                Result = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, DocumentId, Name, ContentType, Hash, ExpectedChangeVector, Stream);
                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new MergedPutAttachmentCommandDto
                {
                    DocumentId = DocumentId,
                    Name = Name,
                    ExpectedChangeVector = ExpectedChangeVector,
                    ContentType = ContentType,
                    Stream = Stream,
                    Hash = Hash
                };
            }
        }

        internal sealed class MergedDeleteAttachmentCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            public string DocumentId;
            public string Name;
            public LazyStringValue ExpectedChangeVector;
            public DocumentDatabase Database;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, DocumentId, Name, ExpectedChangeVector, collectionName: out _);
                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new MergedDeleteAttachmentCommandDto
                {
                    DocumentId = DocumentId,
                    Name = Name,
                    ExpectedChangeVector = ExpectedChangeVector
                };
            }
        }
    }

    public sealed class MergedPutAttachmentCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, AttachmentHandler.MergedPutAttachmentCommand>
    {
        public string DocumentId;
        public string Name;
        public LazyStringValue ExpectedChangeVector;
        public string ContentType;
        public Stream Stream;
        public string Hash;

        public AttachmentHandler.MergedPutAttachmentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new AttachmentHandler.MergedPutAttachmentCommand
            {
                DocumentId = DocumentId,
                Name = Name,
                ExpectedChangeVector = ExpectedChangeVector,
                ContentType = ContentType,
                Stream = Stream,
                Hash = Hash,
                Database = database
            };
        }
    }

    internal sealed class MergedDeleteAttachmentCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, AttachmentHandler.MergedDeleteAttachmentCommand>
    {
        public string DocumentId;
        public string Name;
        public LazyStringValue ExpectedChangeVector;

        public AttachmentHandler.MergedDeleteAttachmentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new AttachmentHandler.MergedDeleteAttachmentCommand
            {
                DocumentId = DocumentId,
                Name = Name,
                ExpectedChangeVector = ExpectedChangeVector,
                Database = database
            };
        }
    }
}
