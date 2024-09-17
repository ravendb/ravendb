using System;
using System.Collections.Generic;
using System.Diagnostics;
using Jint;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Patch
{
    public abstract class PatchDocumentCommandBase : DocumentMergedTransactionCommand
    {
        private readonly bool _skipPatchIfChangeVectorMismatch;

        private readonly JsonOperationContext _externalContext;

        protected DocumentDatabase _database;
        private readonly bool _isTest;
        private readonly bool _debugMode;
        protected readonly bool _returnDocument;

        private bool ModifiedDocumentRequired => _externalContext != null || _returnDocument || _isTest || _debugMode;

        protected readonly (PatchRequest Run, BlittableJsonReaderObject Args) _patchIfMissing;
        private readonly BlittableJsonReaderObject _createIfMissing;
        protected readonly (PatchRequest Run, BlittableJsonReaderObject Args) _patch;

        public List<string> DebugOutput { get; private set; }

        public DynamicJsonValue DebugActions { get; private set; }

        protected PatchDocumentCommandBase(
            JsonOperationContext context,
            bool skipPatchIfChangeVectorMismatch,
            (PatchRequest run, BlittableJsonReaderObject args) patch,
            (PatchRequest run, BlittableJsonReaderObject args) patchIfMissing,
            BlittableJsonReaderObject createIfMissing,
            bool isTest,
            bool debugMode,
            bool collectResultsNeeded,
            bool returnDocument)
        {
            _externalContext = collectResultsNeeded ? context : null;
            _patchIfMissing = patchIfMissing;
            _createIfMissing = createIfMissing;
            _patch = patch;
            _skipPatchIfChangeVectorMismatch = skipPatchIfChangeVectorMismatch;
            _isTest = isTest;
            _debugMode = debugMode;
            _returnDocument = returnDocument;
        }

        public List<string> _operationsMsgs = new List<string>();
        
        protected PatchResult ExecuteOnDocument(DocumentsOperationContext context, string id, LazyStringValue expectedChangeVector, ScriptRunner.SingleRun run, ScriptRunner.SingleRun runIfMissing)
        {
            var sp = Stopwatch.StartNew();
            var sp2 = Stopwatch.StartNew();
            
            _database = context.DocumentDatabase;
            run.DebugMode = _debugMode;
            if (runIfMissing != null)
                runIfMissing.DebugMode = _debugMode;

            using (DocumentIdWorker.GetSliceFromId(context.Allocator, id, out Slice lowerId))
            {
                _database.DocumentsStorage.ValidateId(context, lowerId, type: DocumentChangeTypes.Put);
            }

            
            AddOperationToLog(id, sp, sp2,  "Validate document");
            var originalDocument = GetCurrentDocument(context, id, sp, sp2);

            if (expectedChangeVector != null)
            {
                if (originalDocument == null)
                {
                    if (_skipPatchIfChangeVectorMismatch)
                    {
                        return new PatchResult
                        {
                            Status = PatchStatus.Skipped
                        };
                    }

                    throw new ConcurrencyException($"Could not patch document '{id}' because non current change vector was used")
                    {
                        Id = id,
                        ActualChangeVector = null,
                        ExpectedChangeVector = expectedChangeVector
                    };
                }

                if (ChangeVector.CompareVersion(originalDocument.ChangeVector, expectedChangeVector, context) != 0)
                {
                    if (_skipPatchIfChangeVectorMismatch)
                    {
                        return new PatchResult
                        {
                            Status = PatchStatus.Skipped
                        };
                    }

                    throw new ConcurrencyException($"Could not patch document '{id}' because non current change vector was used")
                    {
                        Id = id,
                        ActualChangeVector = originalDocument.ChangeVector,
                        ExpectedChangeVector = expectedChangeVector
                    };
                }
            }

            if (originalDocument == null && runIfMissing == null && _createIfMissing == null)
            {
                return new PatchResult
                {
                    Status = PatchStatus.DocumentDoesNotExist
                };
            }

            object documentInstance = null;
            var args = _patch.Args;
            if (originalDocument == null)
            {
                if (_createIfMissing == null)
                {
                    run = runIfMissing;
                    args = _patchIfMissing.Args;
                    documentInstance = runIfMissing.CreateEmptyObject();
                }
            }
            else
            {
                id = originalDocument.Id; // we want to use the original Id casing
                if (originalDocument.Data != null)
                {
                    sp2.Restart();
                    documentInstance = (BlittableObjectInstance)run.Translate(context, originalDocument).AsObject();
                    AddOperationToLog(id, sp, sp2,  "Translate document");
                }
            }

            try
            {
                // we will to access this value, and the original document data may be changed by
                // the actions of the script, so we translate (which will create a clone) then use
                // that clone later

                JsonOperationContext patchContext = context;

                if (_debugMode && _externalContext != null)
                {
                    // when running in debug mode let's use the external context so we'll be able to read blittables added to DebugActions

                    patchContext = _externalContext;
                }

                sp2.Restart();
                var modifiedDoc = ExecuteScript(context, id, run, patchContext, documentInstance, args);
                AddOperationToLog(id, sp, sp2,  "ExecuteScript on document");

                var result = new PatchResult
                {
                    Status = PatchStatus.NotModified,
                    OriginalDocument = _isTest == false ? null : originalDocument?.Data?.Clone(_externalContext ?? context),
                    ModifiedDocument = ModifiedDocumentRequired == false ? null : modifiedDoc?.Clone(_externalContext ?? context)
                };

                if (modifiedDoc == null)
                {
                    result.Status = PatchStatus.Skipped;
                    return result;
                }

                if (run?.RefreshOriginalDocument == true)
                {
                    originalDocument?.Dispose();
                    originalDocument = GetCurrentDocument(context, id, sp, sp2, "RefreshOriginalDocument");
                }

                var nonPersistentFlags = HandleMetadataUpdates(context, id, run);

                DocumentsStorage.PutOperationResults? putResult = null;
                if (originalDocument?.Data == null)
                {
                    if (_isTest == false || run?.PutOrDeleteCalled == true || _createIfMissing != null)
                        putResult = _database.DocumentsStorage.Put(context, id, null, modifiedDoc, nonPersistentFlags: nonPersistentFlags);

                    result.Status = PatchStatus.Created;
                }
                else
                {
                    DocumentCompareResult compareResult = default;
                    bool shouldUpdateMetadata = nonPersistentFlags.HasFlag(NonPersistentDocumentFlags.ResolveCountersConflict) ||
                                                nonPersistentFlags.HasFlag(NonPersistentDocumentFlags.ResolveTimeSeriesConflict);

                    if (shouldUpdateMetadata == false)
                    {
                        try
                        {
                            compareResult = DocumentCompare.IsEqualTo(originalDocument.Data, modifiedDoc,
                                DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModification);
                        }
                        catch (InvalidOperationException ioe)
                        {
                            throw new InvalidOperationException($"Could not patch document '{id}'.", ioe);
                        }
                    }

                    if (shouldUpdateMetadata || compareResult != DocumentCompareResult.Equal)
                    {
                        Debug.Assert(originalDocument != null);
                        if (_isTest == false || run.PutOrDeleteCalled)
                        {
                            putResult = _database.DocumentsStorage.Put(
                                context,
                                id,
                                originalDocument.ChangeVector,
                                modifiedDoc,
                                lastModifiedTicks: null,
                                changeVector: null,
                                oldChangeVectorForClusterTransactionIndexCheck: null,
                                originalDocument.Flags.Strip(DocumentFlags.FromClusterTransaction),
                                nonPersistentFlags);
                        }

                        result.Status = PatchStatus.Patched;
                    }
                }

                if (putResult != null)
                {
                    result.ChangeVector = putResult.Value.ChangeVector;
                    result.Collection = putResult.Value.Collection.Name;
                    result.LastModified = putResult.Value.LastModified;
                }

                if (_isTest && result.Status == PatchStatus.NotModified)
                {
                    using (var old = modifiedDoc)
                    {
                        result.ModifiedDocument = originalDocument?.Data?.Clone(_externalContext ?? context);
                    }
                }

                return result;
            }
            finally
            {
                if (run.DebugOutput != null)
                    DebugOutput = new List<string>(run.DebugOutput);

                if (run.DebugActions != null)
                    DebugActions = run.DebugActions.GetDebugActions();

                originalDocument?.Dispose();
                
                AddOperationToLog(id, sp, sp,  "Executed Command Finished");
            }
        }

        private void AddOperationToLog(string id, Stopwatch sp, Stopwatch sp2, string addition)
        {
            if(sp2.Elapsed.TotalMilliseconds > 100)
                _operationsMsgs.Add($"{sp.Elapsed:c} {sp2.Elapsed:c} {id} {addition}");
        }

        protected virtual Document GetCurrentDocument(DocumentsOperationContext context, string id, Stopwatch sp, Stopwatch sp2, string refreshoriginaldocument = null)
        {
            sp2.Restart();
            var originalDocument = _database.DocumentsStorage.Get(context, id);
            AddOperationToLog(id, sp, sp,  $"{refreshoriginaldocument} Validate document");

            if (originalDocument != null)
            {
                using (var oldData = originalDocument.Data)
                {
                    sp2.Restart();
                    // we clone it, to keep it safe from defrag due to the patch modifications
                    originalDocument.Data = originalDocument.Data?.CloneOnTheSameContext();
                    AddOperationToLog(id, sp, sp,  $"{refreshoriginaldocument} Clone document");
                }
            }

            return originalDocument;
        }

        private BlittableJsonReaderObject ExecuteScript(DocumentsOperationContext context, string id, ScriptRunner.SingleRun run, JsonOperationContext patchContext,
            object documentInstance, BlittableJsonReaderObject args)
        {
            if (documentInstance == null)
            {
                return _createIfMissing;
            }

            using (var scriptResult = run.Run(patchContext, context, "execute", id, new[] { documentInstance, args }))
            {
                return scriptResult.TranslateToObject(context, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
        }

        private NonPersistentDocumentFlags HandleMetadataUpdates(DocumentsOperationContext context, string id, ScriptRunner.SingleRun run)
        {
            var nonPersistentFlags = AddResolveFlagOrUpdateRelatedDocuments(context, id, run.DocumentCountersToUpdate, resolveFlag: NonPersistentDocumentFlags.ResolveCountersConflict);
            nonPersistentFlags |= AddResolveFlagOrUpdateRelatedDocuments(context, id, run.DocumentTimeSeriesToUpdate, resolveFlag: NonPersistentDocumentFlags.ResolveTimeSeriesConflict);

            return nonPersistentFlags;
        }

        private NonPersistentDocumentFlags AddResolveFlagOrUpdateRelatedDocuments(DocumentsOperationContext context, string documentId, IEnumerable<string> documentsToUpdate,
            NonPersistentDocumentFlags resolveFlag)
        {
            var nonPersistentFlags = NonPersistentDocumentFlags.None;
            if (documentsToUpdate == null)
                return nonPersistentFlags;

            foreach (var id in documentsToUpdate)
            {
                if (id.Equals(documentId, StringComparison.OrdinalIgnoreCase))
                {
                    nonPersistentFlags |= resolveFlag;
                    continue;
                }

                if (_isTest == false)
                {
                    var docToUpdate = _database.DocumentsStorage.Get(context, id);
                    if (docToUpdate == null)
                        continue;

                    var flags = docToUpdate.Flags.Strip(DocumentFlags.FromClusterTransaction);
                    _database.DocumentsStorage.Put(context, docToUpdate.Id, null, docToUpdate.Data, flags: flags, nonPersistentFlags: resolveFlag);
                }
            }

            return nonPersistentFlags;
        }

        protected string HandleReply(string id, PatchResult patchResult, DynamicJsonArray reply, HashSet<string> modifiedCollections)
        {
            if (patchResult.ModifiedDocument != null)
                _database.HugeDocuments.AddIfDocIsHuge(id, patchResult.ModifiedDocument.Size);

            if (patchResult.Collection != null)
                modifiedCollections?.Add(patchResult.Collection);

            var patchReply = new DynamicJsonValue
            {
                [nameof(BatchRequestParser.CommandData.Id)] = id,
                [nameof(BatchRequestParser.CommandData.ChangeVector)] = patchResult.ChangeVector,
                [nameof(Constants.Documents.Metadata.LastModified)] = patchResult.LastModified,
                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.PATCH),
                [nameof(PatchStatus)] = patchResult.Status,
                [nameof(PatchResult.Debug)] = patchResult.Debug
            };

            if (_returnDocument)
            {
                patchReply[nameof(PatchResult.ModifiedDocument)] = patchResult.ModifiedDocument;
            }

            reply?.Add(patchReply);

            return patchResult.ChangeVector;
        }

        protected void FillDto(PatchDocumentCommandDtoBase dto)
        {
            dto.SkipPatchIfChangeVectorMismatch = _skipPatchIfChangeVectorMismatch;
            dto.Patch = _patch;
            dto.PatchIfMissing = _patchIfMissing;
            dto.CreateIfMissing = _createIfMissing;
            dto.IsTest = _isTest;
            dto.DebugMode = _debugMode;
            dto.CollectResultsNeeded = _externalContext != null;
        }

        public abstract string HandleReply(DynamicJsonArray reply, HashSet<string> modifiedCollections);
    }

    public sealed class BatchPatchDocumentCommand : PatchDocumentCommandBase
    {
        private readonly BlittableJsonReaderArray _ids;

        private readonly List<(string Id, PatchResult PatchResult)> _patchResults = new List<(string Id, PatchResult PatchResult)>();

        public BatchPatchDocumentCommand(
            JsonOperationContext context,
            BlittableJsonReaderArray ids,
            bool skipPatchIfChangeVectorMismatch,
            (PatchRequest run, BlittableJsonReaderObject args) patch,
            (PatchRequest run, BlittableJsonReaderObject args) patchIfMissing,
            BlittableJsonReaderObject createIfMissing,
            bool isTest,
            bool debugMode,
            bool collectResultsNeeded) : base(context, skipPatchIfChangeVectorMismatch, patch, patchIfMissing, createIfMissing, isTest, debugMode, collectResultsNeeded, returnDocument: false)
        {
            _ids = ids;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            if (_ids == null || _ids.Length == 0)
                return 0;

            ScriptRunner.SingleRun runIfMissing = null;
            _database = context.DocumentDatabase;
            using (_database.Scripts.GetScriptRunner(_patch.Run, readOnly: false, out var run))
            using (_patchIfMissing.Run != null ? _database.Scripts.GetScriptRunner(_patchIfMissing.Run, readOnly: false, out runIfMissing) : (IDisposable)null)
            {
                foreach (var item in _ids)
                {
                    if (!(item is BlittableJsonReaderObject bjro))
                        throw new InvalidOperationException();

                    if (bjro.TryGet(nameof(ICommandData.Id), out string id) == false)
                        throw new InvalidOperationException();

                    bjro.TryGet(nameof(ICommandData.ChangeVector), out LazyStringValue expectedChangeVector);

                    var patchResult = ExecuteOnDocument(context, id, expectedChangeVector, run, runIfMissing);
                    _patchResults.Add((id, patchResult));
                }
            }

            return _ids.Length;
        }

        public override string HandleReply(DynamicJsonArray reply, HashSet<string> modifiedCollections)
        {
            reply.Add(new DynamicJsonValue
            {
                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.BatchPATCH)
            });

            return null;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, BatchPatchDocumentCommand> ToDto(DocumentsOperationContext context)
        {
            var dto = new BatchPatchDocumentCommandDto();
            FillDto(dto);

            dto.Ids = _ids;

            return dto;
        }
    }

    public class PatchDocumentCommand : PatchDocumentCommandBase, IDisposable
    {
        private readonly string _id;
        private readonly LazyStringValue _expectedChangeVector;
        private readonly bool _ignoreMaxStepsForScript;
        private ScriptRunner.SingleRun _run;
        private ScriptRunner.SingleRun _runIfMissing;
        private ScriptRunner.ReturnRun _returnRun;
        private IDisposable _disposableStatement;
        private IDisposable _disposableScriptRunner;
        private bool _isInitialized;

        public PatchResult PatchResult { get; private set; }

        public PatchDocumentCommand(
            JsonOperationContext context,
            string id,
            LazyStringValue expectedChangeVector,
            bool skipPatchIfChangeVectorMismatch,
            (PatchRequest run, BlittableJsonReaderObject args) patch,
            (PatchRequest run, BlittableJsonReaderObject args) patchIfMissing,
            BlittableJsonReaderObject createIfMissing,
            char identityPartsSeparator,
            bool isTest,
            bool debugMode,
            bool collectResultsNeeded,
            bool returnDocument,
            bool ignoreMaxStepsForScript = false) : base(context, skipPatchIfChangeVectorMismatch, patch, patchIfMissing, createIfMissing, isTest, debugMode, collectResultsNeeded, returnDocument)
        {
            _id = id;
            _isInitialized = false;
            _expectedChangeVector = expectedChangeVector;
            _ignoreMaxStepsForScript = ignoreMaxStepsForScript;
            if (string.IsNullOrEmpty(id) || id.EndsWith(identityPartsSeparator) || id.EndsWith('|'))
                throw new ArgumentException($"The ID argument has invalid value: '{id}'", nameof(id));

            // If the caller is a DocumentsOperationContext, then we can apply the optimization.
            if (context is DocumentsOperationContext doContext)
                InitializeCmd(doContext);
        }

        private void InitializeCmd(DocumentsOperationContext context)
        {
            _database = context.DocumentDatabase;
            _returnRun = _database.Scripts.GetScriptRunner(_patch.Run, readOnly: false, out _run);
            _disposableStatement = _ignoreMaxStepsForScript ? _run.ScriptEngine.DisableMaxStatements() : null;
            _disposableScriptRunner = _patchIfMissing.Run != null ? _database.Scripts.GetScriptRunner(_patchIfMissing.Run, readOnly: false, out _runIfMissing) : null;
            _isInitialized = true;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            // PERF: Since we were not able to apply the optimization of shifting the cost of the operations into the constructor
            // we will do it here instead. 
            if (_isInitialized == false)
                InitializeCmd(context);

            Debug.Assert(context.DocumentDatabase == _database);

            PatchResult = ExecuteOnDocument(context, _id, _expectedChangeVector, _run, _runIfMissing);

            _returnRun.Dispose();
            _returnRun = default;

            _disposableStatement?.Dispose();
            _disposableStatement = null;

            _disposableScriptRunner?.Dispose();
            _disposableScriptRunner = null;

            return 1;
        }

        public override string HandleReply(DynamicJsonArray reply, HashSet<string> modifiedCollections)
        {
            return HandleReply(_id, PatchResult, reply, modifiedCollections);
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
        {
            var dto = new PatchDocumentCommandDto();
            FillDto(dto);

            dto.Id = _id;
            dto.ExpectedChangeVector = _expectedChangeVector;
            dto.ReturnDocument = _returnDocument;

            return dto;
        }

        public void Dispose()
        {
            _returnRun.Dispose();
            _disposableStatement?.Dispose();
            _disposableScriptRunner?.Dispose();

            PatchResult?.Dispose();
        }
    }

    public sealed class BatchPatchDocumentCommandDto : PatchDocumentCommandDtoBase<BatchPatchDocumentCommand>
    {
        public BlittableJsonReaderArray Ids;

        public override BatchPatchDocumentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new BatchPatchDocumentCommand(
                context,
                Ids,
                SkipPatchIfChangeVectorMismatch,
                Patch,
                PatchIfMissing,
                CreateIfMissing,
                IsTest,
                DebugMode,
                CollectResultsNeeded);
        }
    }

    public sealed class PatchDocumentCommandDto : PatchDocumentCommandDtoBase<PatchDocumentCommand>
    {
        public string Id;
        public LazyStringValue ExpectedChangeVector;
        public bool ReturnDocument;

        public override PatchDocumentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new PatchDocumentCommand(
                context,
                Id,
                ExpectedChangeVector,
                SkipPatchIfChangeVectorMismatch,
                Patch,
                PatchIfMissing,
                CreateIfMissing,
                database.IdentityPartsSeparator,
                IsTest,
                DebugMode,
                CollectResultsNeeded,
                ReturnDocument);
        }
    }

    public abstract class PatchDocumentCommandDtoBase<TCommand> : PatchDocumentCommandDtoBase, IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, TCommand>
        where TCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        public abstract TCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database);
    }

    public abstract class PatchDocumentCommandDtoBase
    {
        public bool SkipPatchIfChangeVectorMismatch;
        public (PatchRequest run, BlittableJsonReaderObject args) Patch;
        public (PatchRequest run, BlittableJsonReaderObject args) PatchIfMissing;
        public BlittableJsonReaderObject CreateIfMissing;
        public bool IsTest;
        public bool DebugMode;
        public bool CollectResultsNeeded;
    }
}

