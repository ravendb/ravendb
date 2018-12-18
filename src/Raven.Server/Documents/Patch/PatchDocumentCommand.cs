using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Jint.Native;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch
{
    public abstract class PatchDocumentCommandBase : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly bool _skipPatchIfChangeVectorMismatch;

        private readonly JsonOperationContext _externalContext;

        protected readonly DocumentDatabase _database;
        private readonly bool _isTest;
        private readonly bool _debugMode;
        protected readonly bool _returnDocument;

        protected readonly (PatchRequest Run, BlittableJsonReaderObject Args) _patchIfMissing;
        protected readonly (PatchRequest Run, BlittableJsonReaderObject Args) _patch;

        public List<string> DebugOutput { get; private set; }

        public DynamicJsonValue DebugActions { get; private set; }

        protected PatchDocumentCommandBase(
            JsonOperationContext context,
            bool skipPatchIfChangeVectorMismatch,
            (PatchRequest run, BlittableJsonReaderObject args) patch,
            (PatchRequest run, BlittableJsonReaderObject args) patchIfMissing,
            DocumentDatabase database,
            bool isTest,
            bool debugMode,
            bool collectResultsNeeded,
            bool returnDocument)
        {
            _externalContext = collectResultsNeeded ? context : null;
            _patchIfMissing = patchIfMissing;
            _patch = patch;
            _skipPatchIfChangeVectorMismatch = skipPatchIfChangeVectorMismatch;
            _database = database;
            _isTest = isTest;
            _debugMode = debugMode;
            _returnDocument = returnDocument;
        }

        protected PatchResult ExecuteOnDocument(DocumentsOperationContext context, string id, LazyStringValue expectedChangeVector, ScriptRunner.SingleRun run, ScriptRunner.SingleRun runIfMissing)
        {
            run.DebugMode = _debugMode;
            if (runIfMissing != null)
                runIfMissing.DebugMode = _debugMode;

            var originalDocument = _database.DocumentsStorage.Get(context, id);
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
                        ActualChangeVector = null,
                        ExpectedChangeVector = expectedChangeVector
                    };
                }

                if (originalDocument.ChangeVector.CompareTo(expectedChangeVector) != 0)
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
                        ActualChangeVector = originalDocument.ChangeVector,
                        ExpectedChangeVector = expectedChangeVector
                    };
                }
            }


            {
                if (originalDocument == null && runIfMissing == null)
                {
                    return new PatchResult
                    {
                        Status = PatchStatus.DocumentDoesNotExist
                    };
                }

                object documentInstance;
                var args = _patch.Args;
                BlittableJsonReaderObject originalDoc;
                if (originalDocument == null)
                {
                    run = runIfMissing;
                    args = _patchIfMissing.Args;
                    documentInstance = runIfMissing.CreateEmptyObject();
                    originalDoc = null;
                }
                else
                {
                    documentInstance = UpdateOriginalDocument();
                }

                try
                {
                    // we will to access this value, and the original document data may be changed by
                    // the actions of the script, so we translate (which will create a clone) then use
                    // that clone later
                    using (var scriptResult = run.Run(context, context, "execute", id, new[] { documentInstance, args }))
                    {
                        var modifiedDocument = scriptResult.TranslateToObject(_externalContext ?? context, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                        var result = new PatchResult
                        {
                            Status = PatchStatus.NotModified,
                            OriginalDocument = _isTest == false ? null : originalDoc?.Clone(context),
                            ModifiedDocument = modifiedDocument
                        };

                        if (modifiedDocument == null)
                        {
                            result.Status = PatchStatus.Skipped;
                            return result;
                        }

                        if (run.RefreshOriginalDocument)
                        {
                            originalDocument = _database.DocumentsStorage.Get(context, id);
                            documentInstance = UpdateOriginalDocument();
                        }

                        if (run.UpdatedDocumentCounterIds != null)
                        {
                            foreach (var docId in run.UpdatedDocumentCounterIds)
                            {
                                if (docId.Equals(id, StringComparison.OrdinalIgnoreCase))
                                {
                                    Debug.Assert(originalDocument != null);
                                    modifiedDocument = UpdateCountersInMetadata(context, modifiedDocument, docId, ref originalDocument.Flags);
                                }
                                else
                                {
                                    var docToUpdate = _database.DocumentsStorage.Get(context, docId);
                                    var docBlittableToUpdate = UpdateCountersInMetadata(context, docToUpdate.Data, docId, ref docToUpdate.Flags);
                                    if (_isTest == false)
                                    {
                                        _database.DocumentsStorage.Put(context, docId,
                                            docToUpdate.ChangeVector, docBlittableToUpdate, null, null, docToUpdate.Flags);
                                    }
                                }
                            }
                        }

                        DocumentsStorage.PutOperationResults? putResult = null;

                        if (originalDoc == null)
                        {
                            if (_isTest == false || run.PutOrDeleteCalled)
                                putResult = _database.DocumentsStorage.Put(context, id, null, modifiedDocument);

                            result.Status = PatchStatus.Created;
                        }
                        else if (DocumentCompare.IsEqualTo(originalDoc, modifiedDocument, tryMergeMetadataConflicts: true) != DocumentCompareResult.Equal)
                        {
                            Debug.Assert(originalDocument != null);
                            if (_isTest == false || run.PutOrDeleteCalled)
                            {
                                putResult = _database.DocumentsStorage.Put(context, originalDocument.Id,
                                    originalDocument.ChangeVector, modifiedDocument, null, null, originalDocument.Flags);
                            }

                            result.Status = PatchStatus.Patched;
                        }

                        if (putResult != null)
                        {
                            result.ChangeVector = putResult.Value.ChangeVector;
                            result.Collection = putResult.Value.Collection.Name;
                            result.LastModified = putResult.Value.LastModified;
                        }

                        return result;
                    }
                }
                finally
                {
                    if (run.DebugOutput != null)
                        DebugOutput = new List<string>(run.DebugOutput);

                    if (run.DebugActions != null)
                        DebugActions = run.DebugActions.GetDebugActions();
                }

                BlittableObjectInstance UpdateOriginalDocument()
                {
                    originalDoc = null;

                    if (originalDocument != null)
                    {
                        var translated = (BlittableObjectInstance)((JsValue)run.Translate(context, originalDocument)).AsObject();
                        // here we need to use the _cloned_ version of the document, since the patch may
                        // change it
                        originalDoc = translated.Blittable;
                        originalDocument.Data = null; // prevent access to this by accident

                        return translated;
                    }

                    return null;
                }
            }
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
                patchReply[nameof(PatchResult.ModifiedDocument)] = patchResult.ModifiedDocument;

            reply.Add(patchReply);

            return patchResult.ChangeVector;
        }

        protected void FillDto(PatchDocumentCommandDtoBase dto)
        {
            dto.SkipPatchIfChangeVectorMismatch = _skipPatchIfChangeVectorMismatch;
            dto.Patch = _patch;
            dto.PatchIfMissing = _patchIfMissing;
            dto.IsTest = _isTest;
            dto.DebugMode = _debugMode;
            dto.CollectResultsNeeded = _externalContext != null;
        }

        public abstract string HandleReply(DynamicJsonArray reply, HashSet<string> modifiedCollections);

        private static BlittableJsonReaderObject UpdateCountersInMetadata(
            DocumentsOperationContext context,
            BlittableJsonReaderObject modifiedDocument,
            string id, ref DocumentFlags flags)
        {
            var metadata = modifiedDocument.GetMetadata();
            if (metadata.Modifications == null)
                metadata.Modifications = new DynamicJsonValue(metadata);

            var countersFromStorage = context.DocumentDatabase.DocumentsStorage.CountersStorage.GetCountersForDocument(context, id).ToList();
            if (countersFromStorage.Count == 0)
            {
                metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                flags &= ~DocumentFlags.HasCounters;
            }
            else
            {
                metadata.Modifications[Constants.Documents.Metadata.Counters] = new DynamicJsonArray(countersFromStorage);
                flags |= DocumentFlags.HasCounters;
            }

            modifiedDocument.Modifications = new DynamicJsonValue(modifiedDocument)
            { [Constants.Documents.Metadata.Key] = metadata };

            modifiedDocument = context.ReadObject(modifiedDocument, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            return modifiedDocument;
        }
    }

    public class BatchPatchDocumentCommand : PatchDocumentCommandBase
    {
        private readonly BlittableJsonReaderArray _ids;

        private readonly List<(string Id, PatchResult PatchResult)> _patchResults = new List<(string Id, PatchResult PatchResult)>();

        public BatchPatchDocumentCommand(
            JsonOperationContext context,
            BlittableJsonReaderArray ids,
            bool skipPatchIfChangeVectorMismatch,
            (PatchRequest run, BlittableJsonReaderObject args) patch,
            (PatchRequest run, BlittableJsonReaderObject args) patchIfMissing,
            DocumentDatabase database,
            bool isTest,
            bool debugMode,
            bool collectResultsNeeded) : base(context, skipPatchIfChangeVectorMismatch, patch, patchIfMissing, database, isTest, debugMode, collectResultsNeeded, returnDocument: false)
        {
            _ids = ids;
        }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            if (_ids == null || _ids.Length == 0)
                return 0;

            ScriptRunner.SingleRun runIfMissing = null;
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

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            var dto = new BatchPatchDocumentCommandDto();
            FillDto(dto);

            dto.Ids = _ids;

            return dto;
        }
    }

    public class PatchDocumentCommand : PatchDocumentCommandBase
    {
        private readonly string _id;
        private readonly LazyStringValue _expectedChangeVector;

        public PatchResult PatchResult { get; private set; }

        public PatchDocumentCommand(
            JsonOperationContext context,
            string id,
            LazyStringValue expectedChangeVector,
            bool skipPatchIfChangeVectorMismatch,
            (PatchRequest run, BlittableJsonReaderObject args) patch,
            (PatchRequest run, BlittableJsonReaderObject args) patchIfMissing,
            DocumentDatabase database,
            bool isTest,
            bool debugMode,
            bool collectResultsNeeded,
            bool returnDocument) : base(context, skipPatchIfChangeVectorMismatch, patch, patchIfMissing, database, isTest, debugMode, collectResultsNeeded, returnDocument)
        {
            _id = id;
            _expectedChangeVector = expectedChangeVector;

            if (string.IsNullOrEmpty(id) || id.EndsWith('/') || id.EndsWith('|'))
                throw new ArgumentException($"The ID argument has invalid value: '{id}'", nameof(id));
        }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            ScriptRunner.SingleRun runIfMissing = null;

            using (_database.Scripts.GetScriptRunner(_patch.Run, readOnly: false, out var run))
            using (_patchIfMissing.Run != null ? _database.Scripts.GetScriptRunner(_patchIfMissing.Run, readOnly: false, out runIfMissing) : (IDisposable)null)
            {
                PatchResult = ExecuteOnDocument(context, _id, _expectedChangeVector, run, runIfMissing);
                return 1;
            }
        }

        public override string HandleReply(DynamicJsonArray reply, HashSet<string> modifiedCollections)
        {
            return HandleReply(_id, PatchResult, reply, modifiedCollections);
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            var dto = new PatchDocumentCommandDto();
            FillDto(dto);

            dto.Id = _id;
            dto.ExpectedChangeVector = _expectedChangeVector;
            dto.ReturnDocument = _returnDocument;

            return dto;
        }
    }

    public class BatchPatchDocumentCommandDto : PatchDocumentCommandDtoBase<BatchPatchDocumentCommand>
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
                database,
                IsTest,
                DebugMode,
                CollectResultsNeeded);
        }
    }

    public class PatchDocumentCommandDto : PatchDocumentCommandDtoBase<PatchDocumentCommand>
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
                database,
                IsTest,
                DebugMode,
                CollectResultsNeeded,
                ReturnDocument);
        }
    }

    public abstract class PatchDocumentCommandDtoBase<TCommand> : PatchDocumentCommandDtoBase, TransactionOperationsMerger.IReplayableCommandDto<TCommand>
        where TCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        public abstract TCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database);
    }

    public abstract class PatchDocumentCommandDtoBase
    {
        public bool SkipPatchIfChangeVectorMismatch;
        public (PatchRequest run, BlittableJsonReaderObject args) Patch;
        public (PatchRequest run, BlittableJsonReaderObject args) PatchIfMissing;
        public bool IsTest;
        public bool DebugMode;
        public bool CollectResultsNeeded;
    }
}

