// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
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
                await processor.ExecuteAsync().ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/docs/size", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetDocSize()
        {
            using (var processor = new DocumentHandlerProcessorForGetDocSize(this))
            {
                await processor.ExecuteAsync().ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/docs", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (var processor = new DocumentHandlerProcessorForGet(HttpMethod.Get, this))
            {
                await processor.ExecuteAsync().ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostGet()
        {
            using (var processor = new DocumentHandlerProcessorForGet(HttpMethod.Post, this))
            {
                await processor.ExecuteAsync().ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/docs", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Delete()
        {
            using (var processor = new DocumentHandlerProcessorForDelete(this))
            {
                await processor.ExecuteAsync().ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/docs", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Put()
        {
            using (var processor = new DocumentHandlerProcessorForPut(this))
            {
                await processor.ExecuteAsync().ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/docs", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Patch()
        {
            using (var processor = new DocumentHandlerProcessorForPatch(this))
            {
                await processor.ExecuteAsync().ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/docs/class", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GenerateClassFromDocument()
        {
            using (var processor = new DocumentHandlerProcessorForGenerateClassFromDocument(this))
            {
                await processor.ExecuteAsync().ConfigureAwait(false);
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

        public static string GenerateNonConflictingId(DocumentDatabase database, string prefix)
        {
            return prefix + database.DocumentsStorage.GenerateNextEtag().ToString("D19") + "-" + Guid.NewGuid().ToBase64Unpadded();
        }

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
            catch (Voron.Exceptions.VoronConcurrencyErrorException)
            {
                // RavenDB-10581 - If we have a concurrency error on "doc-id/"
                // this means that we have existing values under the current etag
                // we'll generate a new (random) id for them.

                // The TransactionMerger will re-run us when we ask it to as a
                // separate transaction
                if (_id?.EndsWith(_database.IdentityPartsSeparator) == true)
                {
                    _id = GenerateNonConflictingId(_database, _id);
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
                    if (_database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice) < 1)
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
    
    public sealed class MergedPutEmbeddingsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
    {
        private readonly AiEtlScriptRun _items;
        private readonly string _configurationName;
        private readonly DocumentDatabase _database;
        public DocumentsStorage.PutOperationResults PutResult;
        
        public MergedPutEmbeddingsCommand(AiEtlScriptRun items, string configurationName, DocumentDatabase database)
        {
            _items = items;
            _configurationName = configurationName;
            _database = database;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            // textual value -> attachment name
            var embeddingsTracker = new Dictionary<string, string>();
            
            foreach (var item in _items.Additions)
            {
                var configDjv = new DynamicJsonValue();
                
                foreach (var kvp in item.Values)
                {
                    var valuePath = kvp.Key;
                    var values = kvp.Value;

                    var attachmentNamesDja = new DynamicJsonArray();
                    
                    foreach (var value in values)
                    {
                        if (embeddingsTracker.TryGetValue(value.TextualValue, out var attachmentName) == false)
                        {
                            attachmentName = _database.AiStorage.AddOrUpdateValueEmbeddingsDocument(context, value);
                            embeddingsTracker.Add(value.TextualValue, attachmentName);
                        }
                        
                        value.ValueEmbeddingsAttachmentName = attachmentName;
                        attachmentNamesDja.Add(attachmentName);
                    }
                    
                    configDjv[valuePath] = attachmentNamesDja;
                }

                var documentEmbeddings = _database.AiStorage.GetDocumentEmbeddings(context, item.DocumentId, out string documentEmbeddingsId);

                DynamicJsonValue documentDjv;
                
                if (documentEmbeddings == null)
                {
                    documentDjv = new DynamicJsonValue
                    {
                        [_configurationName] = configDjv,
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue()
                        {
                            // todo cache
                            [Constants.Documents.Metadata.Collection] = AiHelper.GetDocumentEmbeddingsCollectionName(item.DocumentCollectionName),
                            // todo cache baseline datetime?
                            [Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddMonths(3)
                        }
                    };
                }
                else
                    documentDjv = documentEmbeddings.Data.Modifications;
                
                documentDjv[_configurationName] = configDjv;
                
                using (var bjro = context.ReadObject(documentDjv, documentEmbeddingsId))
                {
                    // todo change vector?
                    _database.DocumentsStorage.Put(context, documentEmbeddingsId, null, bjro);

                    foreach (var kvp in item.Values)
                    {
                        foreach (var itemValue in kvp.Value)
                        {
                            // todo optimize
                            var attachment = _database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, itemValue.ValueEmbeddingsDocumentId,
                                itemValue.ValueEmbeddingsAttachmentName, AttachmentType.Document, null);

                            using (var stream = attachment.Stream)
                            {
                                _database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, documentEmbeddingsId, attachment.Name, attachment.ContentType, attachment.Base64Hash.ToString(), null, stream);
                            }
                        }
                    }
                }
            }

            foreach (var item in _items.Deletes)
            {
                var documentEmbeddingsToDeleteId = AiHelper.GetDocumentEmbeddingsId(item.DocumentId);

                _database.DocumentsStorage.Delete(context, documentEmbeddingsToDeleteId, DocumentFlags.None);
            }
            
            return _items.Additions.Count + _items.Deletes.Count;
        }
        
        public void Dispose()
        {
            
        }

        // todo
        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }

        public sealed class MergedPutEmbeddingsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedPutCommand>
        {
            public string Id { get; set; }
            public LazyStringValue ExpectedChangeVector { get; set; }
            public BlittableJsonReaderObject Document { get; set; }

            public MergedPutCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                throw new NotImplementedException();
            }
        }
    }
}
