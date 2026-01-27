using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Handlers
{
    public sealed class DocumentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/docs", "HEAD", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Head()
        {
            using (var processor = new DocumentHandlerProcessorForHead(this))
            {
                await processor.ExecuteAsync();
            }
        }
        
        [RavenAction("/databases/*/docs/size", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetDocSize()
        {
            using (var processor = new DocumentHandlerProcessorForGetDocSize(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/docs", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task Get()
        {
            // Disposal of the processor is done by having it auto-register to the request context disposal mechanism
            return new DocumentHandlerProcessorForGet(HttpMethod.Get, this).ExecuteAsync().AsTask();
        }

        [RavenAction("/databases/*/docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostGet()
        {
            DocumentsOperationContext context = GetContextScopedToRequest();
            List<ReadOnlyMemory<char>> ids = await DocumentHandlerProcessorForGet.GetIdsFromRequestBodyAsync(context, this);
            
            // Disposal of the processor is done by having it auto-register to the request context disposal mechanism
            await new DocumentHandlerProcessorForGet(HttpMethod.Post, this, ids).ExecuteAsync();
        }

        [RavenAction("/databases/*/docs", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Delete()
        {
            using (var processor = new DocumentHandlerProcessorForDelete(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/docs", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Put()
        {
            using (var processor = new DocumentHandlerProcessorForPut(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/docs", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Patch()
        {
            using (var processor = new DocumentHandlerProcessorForPatch(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/docs/class", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GenerateClassFromDocument()
        {
            using (var processor = new DocumentHandlerProcessorForGenerateClassFromDocument(this))
            {
                await processor.ExecuteAsync();
            }
        }

    }

    public sealed class MergedPutCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
    {
        private string _id;
        private readonly LazyStringValue _expectedChangeVector;
        private readonly BlittableJsonReaderObject _document;
        private readonly DocumentDatabase _database;
        private readonly bool _shouldValidateAttachments;
        public DocumentsStorage.PutOperationResults PutResult;
        
        public MergedPutCommand(BlittableJsonReaderObject doc, string id, LazyStringValue changeVector, DocumentDatabase database, bool shouldValidateAttachments = false)
        {
            _document = doc;
            _id = id;
            _expectedChangeVector = changeVector;
            _database = database;
            _shouldValidateAttachments = shouldValidateAttachments;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            if (_shouldValidateAttachments)
            {
                if (_document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata)
                    && metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments))
                {
                    ValidateAttachments(attachments, context, _id);
                }
            }
            try
            {
                PutResult = _database.DocumentsStorage.Put(context, _id, _expectedChangeVector, _document);
            }
            catch (Voron.Exceptions.VoronConcurrencyErrorException e)
            {
                if (DocumentPutAction.TryHandleVoronConcurrencyError(e, _database, _id, out var newId))
                {
                    _id = newId;
                    // The TransactionMerger will re-run us when we ask it to as a separate transaction
                    RetryOnError = true;
                }
                
                throw;
            }
            return 1;
        }

        private void ValidateAttachments(BlittableJsonReaderArray attachments, DocumentsOperationContext context, string id)
        {
            if (attachments == null)
            {
                throw new InvalidOperationException($"Can not put document (id={id}) with '{Constants.Documents.Metadata.Attachments}': null");
            }

            foreach (BlittableJsonReaderObject attachment in attachments)
            {
                if (attachment.TryGet(nameof(AttachmentName.Hash), out string hash) == false || hash == null)
                {
                    throw new InvalidOperationException($"Can not put document (id={id}) because it contains an attachment without an hash property.");
                }
                using (Slice.From(context.Allocator, hash, out var hashSlice))
                {
                    if (_database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice).LocalAttachmentsCount < 1)
                    {
                        throw new InvalidOperationException($"Can not put document (id={id}) because it contains an attachment with hash={hash} but no such attachment is stored.");
                    }
                }
            }
        }

        public void Dispose()
        {
            _document?.Dispose();
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new MergedPutCommandDto
            {
                Id = _id,
                ExpectedChangeVector = _expectedChangeVector,
                Document = _document
            };
        }

        public sealed class MergedPutCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedPutCommand>
        {
            public string Id { get; set; }
            public LazyStringValue ExpectedChangeVector { get; set; }
            public BlittableJsonReaderObject Document { get; set; }

            public MergedPutCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new MergedPutCommand(Document, Id, ExpectedChangeVector, database);
            }
        }
    }
}
