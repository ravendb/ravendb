// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;
using Constants = Raven.Client.Constants;
using DeleteDocumentCommand = Raven.Server.Documents.TransactionCommands.DeleteDocumentCommand;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/docs", "HEAD", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Head()
        {
            using (var processor = new DocumentHandlerProcessorForHead(this, ContextPool))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/docs/size", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetDocSize()
        {
            using (var processor = new DocumentHandlerProcessorForGetDocSize(this, ContextPool))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/docs", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (var processor = new DocumentHandlerProcessorForGet(HttpMethod.Get, this, ContextPool))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostGet()
        {
            using (var processor = new DocumentHandlerProcessorForGet(HttpMethod.Post, this, ContextPool))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/docs", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Delete()
        {
            using (var processor = new DocumentHandlerProcessorForDelete(this, ContextPool))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/docs", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
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
                    var (_, clusterId, _) = await ServerStore.GenerateClusterIdentityAsync(id, Database.IdentityPartsSeparator, Database.Name, GetRaftRequestIdFromQuery());
                    id = clusterId;
                }

                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                using (var cmd = new MergedPutCommand(doc, id, changeVector, Database, shouldValidateAttachments: true))
                {
                    await Database.TxMerger.Enqueue(cmd);

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

        [RavenAction("/databases/*/docs", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Patch()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            var isTest = GetBoolValueQueryString("test", required: false) ?? false;
            var debugMode = GetBoolValueQueryString("debug", required: false) ?? isTest;
            var skipPatchIfChangeVectorMismatch = GetBoolValueQueryString("skipPatchIfChangeVectorMismatch", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var request = await context.ReadForMemoryAsync(RequestBodyStream(), "ScriptedPatchRequest");
                if (request.TryGet("Patch", out BlittableJsonReaderObject patchCmd) == false || patchCmd == null)
                    throw new ArgumentException("The 'Patch' field in the body request is mandatory");

                var patch = PatchRequest.Parse(patchCmd, out var patchArgs);

                PatchRequest patchIfMissing = null;
                BlittableJsonReaderObject patchIfMissingArgs = null;
                if (request.TryGet("PatchIfMissing", out BlittableJsonReaderObject patchIfMissingCmd) && patchIfMissingCmd != null)
                    patchIfMissing = PatchRequest.Parse(patchIfMissingCmd, out patchIfMissingArgs);

                request.TryGet("CreateIfMissing", out BlittableJsonReaderObject createIfMissing);

                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var command = new PatchDocumentCommand(context,
                    id,
                    changeVector,
                    skipPatchIfChangeVectorMismatch,
                    (patch, patchArgs),
                    (patchIfMissing, patchIfMissingArgs),
                    createIfMissing,
                    Database.IdentityPartsSeparator,
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

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(command.PatchResult.Status));
                    writer.WriteString(command.PatchResult.Status.ToString());
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.ModifiedDocument));
                    writer.WriteObject(command.PatchResult.ModifiedDocument);

                    if (debugMode)
                    {
                        writer.WriteComma();
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
                    }

                    switch (command.PatchResult.Status)
                    {
                        case PatchStatus.Created:
                        case PatchStatus.Patched:

                            writer.WriteComma();

                            writer.WritePropertyName(nameof(command.PatchResult.LastModified));
                            writer.WriteString(command.PatchResult.LastModified.GetDefaultRavenFormat());
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(command.PatchResult.ChangeVector));
                            writer.WriteString(command.PatchResult.ChangeVector);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(command.PatchResult.Collection));
                            writer.WriteString(command.PatchResult.Collection);
                            break;
                    }

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/docs/class", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GenerateClassFromDocument()
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
                    return;
                }

                switch (lang)
                {
                    case "csharp":
                        break;

                    default:
                        throw new NotImplementedException($"Document code generator isn't implemented for {lang}");
                }

                await using (var writer = new StreamWriter(ResponseBodyStream()))
                {
                    var codeGenerator = new JsonClassGenerator(lang);
                    var code = codeGenerator.Execute(document.Data);
                    await writer.WriteAsync(code);
                }
            }
        }
    }

    public class MergedPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
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
                    if (AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice) < 1)
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
