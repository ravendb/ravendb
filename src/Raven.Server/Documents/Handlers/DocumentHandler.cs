// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
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
            // POC: Direct handler implementation (v5.4 style) for performance regression testing
            await GetDirect().ConfigureAwait(false);
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

        // POC: Direct handler implementation (v5.4 style) for performance regression testing
        private async Task GetDirect()
        {
            var ids = GetStringValuesQueryString("id", required: false);
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            var includes = GetStringValuesQueryString("include", required: false);
            var counters = GetStringValuesQueryString("counter", required: false);
            var timeSeries = GetStringValuesQueryString("timeSeries", required: false);
            var compareExchange = GetStringValuesQueryString("compareExchange", required: false);
            var revisions = GetStringValuesQueryString("revisions", required: false);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                if (ids.Count > 0)
                {
                    // Handle document GET by ID
                    await HandleGetByIds(context, ids, metadataOnly, includes, counters, timeSeries, compareExchange, revisions).ConfigureAwait(false);
                }
                else
                {
                    // Handle bulk document GET operations
                    await HandleBulkGet(context, metadataOnly).ConfigureAwait(false);
                }
            }
        }

        private async Task HandleGetByIds(DocumentsOperationContext context, StringValues ids, bool metadataOnly, 
            StringValues includes, StringValues counters, StringValues timeSeries, StringValues compareExchange, StringValues revisions)
        {
            var etag = GetStringFromHeaders(Constants.Headers.IfNoneMatch);
            
            using (context.OpenReadTransaction())
            {
                var documents = new List<Document>(ids.Count);
                var includeDocuments = new List<Document>();
                var includeDocs = new IncludeDocumentsCommand(Database.DocumentsStorage, context, includes, isProjection: false);
                
                foreach (var id in ids)
                {
                    if (string.IsNullOrEmpty(id))
                    {
                        documents.Add(null);
                        continue;
                    }

                    var document = Database.DocumentsStorage.Get(context, id);
                    
                    if (document == null && ids.Count == 1)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }
                    
                    documents.Add(document);
                    if (document != null)
                    {
                        includeDocs.Gather(document);
                    }
                }

                // Fill includes
                includeDocs.Fill(includeDocuments, includeMissingAsNull: false);

                // Compute ETag for caching
                var actualEtag = ComputeHttpEtags.ComputeEtagForDocuments(documents, includeDocuments, null, null, null);
                
                // Handle conditional requests
                if (etag == actualEtag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }
                
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{actualEtag}\"";

                // Write response - transaction must remain open while writing
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    
                    // Write Results array
                    writer.WritePropertyName("Results");
                    await writer.WriteDocumentsAsync(context, documents, metadataOnly, CancellationToken.None);
                    
                    // Always write Includes (even if empty) when includes are requested
                    if (includes.Count > 0 || includeDocuments.Count > 0)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName("Includes");
                        await writer.WriteIncludesAsync(context, includeDocuments, CancellationToken.None);
                    }
                    
                    writer.WriteEndObject();
                }
            }
        }

        private async Task HandleBulkGet(DocumentsOperationContext context, bool metadataOnly)
        {
            var changeVector = GetStringFromHeaders(Constants.Headers.IfNoneMatch);
            var etag = GetLongQueryString("etag", required: false);
            var startsWith = GetStringQueryString("startsWith", required: false);
            var startAfter = GetStringQueryString("startAfter", required: false);
            var matches = GetStringQueryString("matches", required: false);
            var exclude = GetStringQueryString("exclude", required: false);
            var start = GetStart();
            var pageSize = GetPageSize();

            using (context.OpenReadTransaction())
            {
                var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);

                if (changeVector == databaseChangeVector)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                IEnumerable<Document> documents;
                if (etag.HasValue)
                {
                    documents = Database.DocumentsStorage.GetDocumentsFrom(context, etag.Value, start, pageSize);
                }
                else if (!string.IsNullOrEmpty(startsWith))
                {
                    documents = Database.DocumentsStorage.GetDocumentsStartingWith(context, startsWith, matches, exclude, startAfter, start, pageSize);
                }
                else
                {
                    documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, start, pageSize);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{databaseChangeVector}\"";

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    await writer.WriteDocumentsAsync(context, documents, metadataOnly, Database.DatabaseShutdown).ConfigureAwait(false);
                    writer.WriteEndObject();
                }
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
}
