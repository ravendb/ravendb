// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using DeleteDocumentCommand = Raven.Server.Documents.TransactionCommands.DeleteDocumentCommand;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/docs", "HEAD", AuthorizationStatus.ValidUser)]
        public Task Head()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var changeVector = GetStringFromHeaders("If-None-Match");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, id);
                if (document == null)
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                else
                {
                    if (changeVector == document.ChangeVector)
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    else
                        HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + document.ChangeVector + "\"";
                }

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/docs/size", "GET", AuthorizationStatus.ValidUser)]
        public Task GetDocSize()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.GetDocumentMetrics(context, id);
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                var documentSizeDetails = new DocumentSizeDetails
                {
                    DocId = id,
                    ActualSize = document.Value.ActualSize,
                    HumaneActualSize = Sizes.Humane(document.Value.ActualSize),
                    AllocatedSize = document.Value.AllocatedSize,
                    HumaneAllocatedSize = Sizes.Humane(document.Value.AllocatedSize)
                };

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, documentSizeDetails.ToJson());
                    writer.Flush();
                }

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/docs", "GET", AuthorizationStatus.ValidUser)]
        public async Task Get()
        {
            var ids = GetStringValuesQueryString("id", required: false);
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (ids.Count > 0)
                    await GetDocumentsByIdAsync(context, ids, metadataOnly);
                else
                    await GetDocumentsAsync(context, metadataOnly);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(ids.ToString(), TrafficWatchChangeType.Documents);
            }
        }

        [RavenAction("/databases/*/docs", "POST", AuthorizationStatus.ValidUser)]
        public async Task PostGet()
        {
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var docs = await context.ReadForMemoryAsync(RequestBodyStream(), "docs");
                if (docs.TryGet("Ids", out BlittableJsonReaderArray array) == false)
                    ThrowRequiredPropertyNameInRequest("Ids");

                var ids = new string[array.Length];
                for (int i = 0; i < array.Length; i++)
                {
                    ids[i] = array.GetStringByIndex(i);
                }

                context.OpenReadTransaction();

                // init here so it can be passed to TW
                var idsStringValues = new StringValues(ids);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(idsStringValues.ToString(), TrafficWatchChangeType.Documents);

                await GetDocumentsByIdAsync(context, idsStringValues, metadataOnly);
            }
        }

        private async Task GetDocumentsAsync(DocumentsOperationContext context, bool metadataOnly)
        {
            var sw = Stopwatch.StartNew();

            // everything here operates on all docs
            var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);

            if (GetStringFromHeaders("If-None-Match") == databaseChangeVector)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }
            HttpContext.Response.Headers["ETag"] = "\"" + databaseChangeVector + "\"";

            var etag = GetLongQueryString("etag", false);
            var start = GetStart();
            var pageSize = GetPageSize();
            var isStartsWith = HttpContext.Request.Query.ContainsKey("startsWith");

            IEnumerable<Document> documents;
            if (etag != null)
            {
                documents = Database.DocumentsStorage.GetDocumentsFrom(context, etag.Value, start, pageSize);
            }
            else if (isStartsWith)
            {
                documents = Database.DocumentsStorage.GetDocumentsStartingWith(context,
                     HttpContext.Request.Query["startsWith"],
                     HttpContext.Request.Query["matches"],
                     HttpContext.Request.Query["exclude"],
                     HttpContext.Request.Query["startAfter"],
                     start,
                     pageSize);
            }
            else // recent docs
            {
                documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, start, pageSize);
            }

            int numberOfResults;

            using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), Database.DatabaseShutdown))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");

                numberOfResults = await writer.WriteDocumentsAsync(context, documents, metadataOnly);

                writer.WriteEndObject();
                await writer.OuterFlushAsync();
            }

            AddPagingPerformanceHint(PagingOperationType.Documents, isStartsWith ? nameof(DocumentsStorage.GetDocumentsStartingWith) : nameof(GetDocumentsAsync), HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds);
        }

        private async Task GetDocumentsByIdAsync(DocumentsOperationContext context, StringValues ids, bool metadataOnly)
        {
            var sw = Stopwatch.StartNew();

            var includePaths = GetStringValuesQueryString("include", required: false);
            var documents = new List<Document>(ids.Count);
            var includes = new List<Document>(includePaths.Count * ids.Count);
            var includeDocs = new IncludeDocumentsCommand(Database.DocumentsStorage, context, includePaths);

            GetCountersQueryString(Database, context, out var includeCounters);

            foreach (var id in ids)
            {
                var document = Database.DocumentsStorage.Get(context, id);
                if (document == null && ids.Count == 1)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                documents.Add(document);
                includeDocs.Gather(document);
                includeCounters?.Fill(document);
            }

            includeDocs.Fill(includes);

            var actualEtag = ComputeHttpEtags.ComputeEtagForDocuments(documents, includes);

            var etag = GetStringFromHeaders("If-None-Match");
            if (etag == actualEtag)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";

            int numberOfResults = 0;

            numberOfResults = await WriteDocumentsJsonAsync(context, metadataOnly, documents, includes, includeCounters?.Results, numberOfResults);

            AddPagingPerformanceHint(PagingOperationType.Documents, nameof(GetDocumentsByIdAsync), HttpContext.Request.QueryString.Value, numberOfResults, documents.Count, sw.ElapsedMilliseconds);
        }

        private void GetCountersQueryString(DocumentDatabase database, DocumentsOperationContext context, out IncludeCountersCommand includeCounters)
        {
            includeCounters = null;

            var counters = GetStringValuesQueryString("counter", required: false);
            if (counters.Count == 0)
                return;

            if (counters.Count == 1 &&
                counters[0] == Constants.Counters.All)
            {
                counters = new string[0];
            }

            includeCounters = new IncludeCountersCommand(database, context, counters);
        }

        private async Task<int> WriteDocumentsJsonAsync(JsonOperationContext context, bool metadataOnly, IEnumerable<Document> documentsToWrite, List<Document> includes, Dictionary<string, List<CounterDetail>> counters, int numberOfResults)
        {
            using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), Database.DatabaseShutdown))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetDocumentsResult.Results));
                numberOfResults = await writer.WriteDocumentsAsync(context, documentsToWrite, metadataOnly);

                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.Includes));
                if (includes.Count > 0)
                {
                    await writer.WriteIncludesAsync(context, includes);
                }
                else
                {
                    writer.WriteStartObject();
                    writer.WriteEndObject();
                }

                if (counters?.Count > 0)
                {
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(GetDocumentsResult.CounterIncludes));
                    await writer.WriteCountersAsync(context, counters);
                }

                writer.WriteEndObject();
                await writer.OuterFlushAsync();
            }
            return numberOfResults;
        }

        [RavenAction("/databases/*/docs", "DELETE", AuthorizationStatus.ValidUser)]
        public async Task Delete()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var cmd = new DeleteDocumentCommand(id, changeVector, Database, catchConcurrencyErrors: true);
                await Database.TxMerger.Enqueue(cmd);
                cmd.ExceptionDispatchInfo?.Throw();
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/docs", "PUT", AuthorizationStatus.ValidUser)]
        public async Task Put()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                // We HAVE to read the document in full, trying to parallelize the doc read
                // and the identity generation needs to take into account that the identity 
                // generation can fail and will leave the reading task hanging if we abort
                // easier to just do in synchronously
                var doc = await context.ReadForDiskAsync(RequestBodyStream(), id).ConfigureAwait(false);

                if (id[id.Length - 1] == '|')
                {
                    var (_, clusterId, _) = await ServerStore.GenerateClusterIdentityAsync(id, Database.Name);
                    id = clusterId;
                }

                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                using (var cmd = new MergedPutCommand(doc, id, changeVector, Database))
                {
                    await Database.TxMerger.Enqueue(cmd);

                    cmd.ExceptionDispatchInfo?.Throw();

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(PutResult.Id));
                        writer.WriteString(cmd.PutResult.Id);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(PutResult.ChangeVector));
                        writer.WriteString(cmd.PutResult.ChangeVector);

                        writer.WriteEndObject();
                    }
                }
            }
        }

        [RavenAction("/databases/*/docs", "PATCH", AuthorizationStatus.ValidUser)]
        public async Task Patch()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            var isTest = GetBoolValueQueryString("test", required: false) ?? false;
            var debugMode = GetBoolValueQueryString("debug", required: false) ?? isTest;
            var skipPatchIfChangeVectorMismatch = GetBoolValueQueryString("skipPatchIfChangeVectorMismatch", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var request = context.Read(RequestBodyStream(), "ScriptedPatchRequest");
                if (request.TryGet("Patch", out BlittableJsonReaderObject patchCmd) == false || patchCmd == null)
                    throw new ArgumentException("The 'Patch' field in the body request is mandatory");

                var patch = PatchRequest.Parse(patchCmd, out var patchArgs);

                PatchRequest patchIfMissing = null;
                BlittableJsonReaderObject patchIfMissingArgs = null;
                if (request.TryGet("PatchIfMissing", out BlittableJsonReaderObject patchIfMissingCmd) && patchIfMissingCmd != null)
                    patchIfMissing = PatchRequest.Parse(patchIfMissingCmd, out patchIfMissingArgs);

                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var command = new PatchDocumentCommand(context,
                    id,
                    changeVector,
                    skipPatchIfChangeVectorMismatch,
                    (patch, patchArgs),
                    (patchIfMissing, patchIfMissingArgs),
                    Database,
                    isTest,
                    debugMode,
                    true,
                    returnDocument: false
                );


                if (isTest == false)
                {
                    await Database.TxMerger.Enqueue(command);
                }
                else
                {
                    // PutDocument requires the write access to the docs storage
                    // testing patching is rare enough not to optimize it
                    using (context.OpenWriteTransaction())
                    {
                        command.Execute(context, null);
                    }
                }

                switch (command.PatchResult.Status)
                {
                    case PatchStatus.DocumentDoesNotExist:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    case PatchStatus.Created:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                        break;
                    case PatchStatus.Skipped:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return;
                    case PatchStatus.Patched:
                    case PatchStatus.NotModified:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(command.PatchResult.Status));
                    writer.WriteString(command.PatchResult.Status.ToString());
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.ModifiedDocument));
                    writer.WriteObject(command.PatchResult.ModifiedDocument);
                    writer.WriteComma();

                    if (debugMode)
                    {
                        writer.WritePropertyName(nameof(command.PatchResult.OriginalDocument));
                        if (isTest)
                            writer.WriteObject(command.PatchResult.OriginalDocument);
                        else
                            writer.WriteNull();

                        writer.WriteComma();

                        writer.WritePropertyName(nameof(command.PatchResult.Debug));

                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Info"] = new DynamicJsonArray(command.DebugOutput),
                            ["Actions"] = command.DebugActions
                        });

                        writer.WriteComma();
                    }

                    writer.WritePropertyName(nameof(command.PatchResult.LastModified));
                    writer.WriteString(command.PatchResult.LastModified.ToString(DefaultFormat.DateTimeFormatsToWrite));
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.ChangeVector));
                    writer.WriteString(command.PatchResult.ChangeVector);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.Collection));
                    writer.WriteString(command.PatchResult.Collection);

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/docs/class", "GET", AuthorizationStatus.ValidUser)]
        public Task GenerateClassFromDocument()
        {
            var id = GetStringQueryString("id");
            var lang = (GetStringQueryString("lang", required: false) ?? "csharp")
                .Trim().ToLowerInvariant();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, id);
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                switch (lang)
                {
                    case "csharp":
                        break;
                    default:
                        throw new NotImplementedException($"Document code generator isn't implemented for {lang}");
                }

                using (var writer = new StreamWriter(ResponseBodyStream()))
                {
                    var codeGenerator = new JsonClassGenerator(lang);
                    var code = codeGenerator.Execute(document);
                    writer.Write(code);
                }

                return Task.CompletedTask;
            }
        }
    }

    public class DocumentSizeDetails : IDynamicJson
    {
        public string DocId { get; set; }
        public int ActualSize { get; set; }
        public string HumaneActualSize { get; set; }
        public int AllocatedSize { get; set; }
        public string HumaneAllocatedSize { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocId)] = DocId,
                [nameof(ActualSize)] = ActualSize,
                [nameof(HumaneActualSize)] = HumaneActualSize,
                [nameof(AllocatedSize)] = AllocatedSize,
                [nameof(HumaneAllocatedSize)] = HumaneAllocatedSize
            };
        }
    }

    public class MergedPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
    {
        private string _id;
        private readonly LazyStringValue _expectedChangeVector;
        private readonly BlittableJsonReaderObject _document;
        private readonly DocumentDatabase _database;

        public ExceptionDispatchInfo ExceptionDispatchInfo;
        public DocumentsStorage.PutOperationResults PutResult;

        public static string GenerateNonConflictingId(DocumentDatabase database, string prefix)
        {
            return prefix + database.DocumentsStorage.GenerateNextEtag().ToString("D19") + "-" + Guid.NewGuid().ToBase64Unpadded();
        }

        public MergedPutCommand(BlittableJsonReaderObject doc, string id, LazyStringValue changeVector, DocumentDatabase database)
        {
            _document = doc;
            _id = id;
            _expectedChangeVector = changeVector;
            _database = database;
        }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
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
                if (_id?.EndsWith('/') == true)
                {
                    _id = GenerateNonConflictingId(_database, _id);
                    RetryOnError = true;
                }
                throw;
            }
            catch (ConcurrencyException e)
            {
                ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }
            return 1;
        }

        public void Dispose()
        {
            _document?.Dispose();
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return new MergedPutCommandDto()
            {
                Id = _id,
                ExpectedChangeVector = _expectedChangeVector,
                Document = _document
            };
        }

        public class MergedPutCommandDto : TransactionOperationsMerger.IReplayableCommandDto<MergedPutCommand>
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
