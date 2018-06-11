using System;
using System.Collections.Generic;
using System.Diagnostics;
using Esprima.Ast;
using Jint.Native;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch
{
    public class PatchDocumentCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable, TransactionOperationsMerger.IRecordableCommand
    {
        private const string IdKey = "Id";
        private const string ExpectedChangeVectorKey = "ExpectedChangeVector";
        private const string SkipPatchIfChangeVectorMismatchKey = "SkipPatchIfChangeVectorMismatch";
        private const string PatchKey = "Patch";
        private const string PatchIfMissingKey = "PatchIfMissing";
        private const string RunKey = "Run";
        private const string ArgKey = "Arg";
        private const string IsTestKey = "IsTest";
        private const string DebugModeKey = "DebugMode";
        private const string CollectResultsNeededKey = "CollectResultsNeeded";


        private readonly string _id;
        private readonly LazyStringValue _expectedChangeVector;
        private readonly bool _skipPatchIfChangeVectorMismatch;

        private readonly JsonOperationContext _externalContext;

        private readonly DocumentDatabase _database;
        private readonly bool _isTest;
        private readonly bool _debugMode;

        private ScriptRunner.SingleRun _run;
        private readonly ScriptRunner.SingleRun _runIfMissing;
        private ScriptRunner.ReturnRun _returnRun;
        private ScriptRunner.ReturnRun _returnRunIfMissing;

        private readonly (PatchRequest run, BlittableJsonReaderObject args) _patchIfMissing;
        private readonly (PatchRequest run, BlittableJsonReaderObject args) _patch;

        public List<string> DebugOutput => _run.DebugOutput;

        public PatchDebugActions DebugActions => _run.DebugActions;

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
            bool collectResultsNeeded)
        {
            _externalContext = collectResultsNeeded ? context : null;
            _patchIfMissing = patchIfMissing;
            _patch = patch;
            _returnRun = database.Scripts.GetScriptRunner(patch.run, false, out _run);
            _run.DebugMode = debugMode;
            if (_runIfMissing != null)
                _runIfMissing.DebugMode = debugMode;
            _returnRunIfMissing = database.Scripts.GetScriptRunner(patchIfMissing.run, false, out _runIfMissing);
            _id = id;
            _expectedChangeVector = expectedChangeVector;
            _skipPatchIfChangeVectorMismatch = skipPatchIfChangeVectorMismatch;
            _database = database;
            _isTest = isTest;
            _debugMode = debugMode;

            if (string.IsNullOrEmpty(id) || id.EndsWith('/') || id.EndsWith('|'))
            {
                throw new ArgumentException("The id argument has invalid value: '" + id + "'", "id");
            }
        }

        public PatchResult PatchResult { get; private set; }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            var originalDocument = _database.DocumentsStorage.Get(context, _id);
            _run.DebugMode = _debugMode;
            if (_expectedChangeVector != null)
            {
                if (originalDocument == null)
                {
                    if (_skipPatchIfChangeVectorMismatch)
                    {
                        PatchResult = new PatchResult
                        {
                            Status = PatchStatus.Skipped
                        };
                        return 1;
                    }

                    throw new ConcurrencyException($"Could not patch document '{_id}' because non current change vector was used")
                    {
                        ActualChangeVector = null,
                        ExpectedChangeVector = _expectedChangeVector
                    };
                }

                if (originalDocument.ChangeVector.CompareTo(_expectedChangeVector) != 0)
                {
                    if (_skipPatchIfChangeVectorMismatch)
                    {
                        PatchResult = new PatchResult
                        {
                            Status = PatchStatus.Skipped
                        };
                        return 1;
                    }

                    throw new ConcurrencyException($"Could not patch document '{_id}' because non current change vector was used")
                    {
                        ActualChangeVector = originalDocument.ChangeVector,
                        ExpectedChangeVector = _expectedChangeVector
                    };
                }
            }

            if (originalDocument == null && _runIfMissing == null)
            {
                PatchResult = new PatchResult
                {
                    Status = PatchStatus.DocumentDoesNotExist
                };
                return 1;
            }

            object documentInstance;
            var args = _patch.args;
            BlittableJsonReaderObject originalDoc;
            if (originalDocument == null)
            {
                _run = _runIfMissing;
                args = _patchIfMissing.args;
                documentInstance = _runIfMissing.CreateEmptyObject();
                originalDoc = null;
            }
            else
            {
                var translated = (BlittableObjectInstance)((JsValue)_run.Translate(context, originalDocument)).AsObject();
                // here we need to use the _cloned_ version of the document, since the patch may
                // change it
                originalDoc = translated.Blittable;
                originalDocument.Data = null; // prevent access to this by accident
                documentInstance = translated;
            }

            // we will to access this value, and the original document data may be changed by
            // the actions of the script, so we translate (which will create a clone) then use
            // that clone later
            using (var scriptResult = _run.Run(context, context, "execute", new[] { documentInstance, args }))
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
                    PatchResult = result;

                    return 1;
                }

                DocumentsStorage.PutOperationResults? putResult = null;

                if (originalDoc == null)
                {
                    if (_isTest == false || _run.PutOrDeleteCalled)
                        putResult = _database.DocumentsStorage.Put(context, _id, null, modifiedDocument);

                    result.Status = PatchStatus.Created;
                }
                else if (DocumentCompare.IsEqualTo(originalDoc, modifiedDocument,
                    tryMergeAttachmentsConflict: true) == DocumentCompareResult.NotEqual)
                {
                    Debug.Assert(originalDocument != null);
                    if (_isTest == false || _run.PutOrDeleteCalled)
                        putResult = _database.DocumentsStorage.Put(context, originalDocument.Id,
                            originalDocument.ChangeVector, modifiedDocument, null, null, originalDocument.Flags);

                    result.Status = PatchStatus.Patched;
                }

                if (putResult != null)
                {
                    result.ChangeVector = putResult.Value.ChangeVector;
                    result.Collection = putResult.Value.Collection.Name;
                }

                PatchResult = result;
                return 1;
            }
        }

        public void Dispose()
        {
            _returnRun.Dispose();
            _returnRunIfMissing.Dispose();
        }

        public DynamicJsonValue Serialize()
        {
            var ret = new DynamicJsonValue
            {
                [IdKey] = _id,
            };

            RecordBool(ret, IsTestKey, _isTest);
            RecordBool(ret, DebugModeKey, _debugMode);
            RecordBool(ret, CollectResultsNeededKey, _externalContext != null);
            RecordBool(ret, SkipPatchIfChangeVectorMismatchKey, _skipPatchIfChangeVectorMismatch);

            if (_expectedChangeVector != null)
            {
                ret[ExpectedChangeVectorKey] = _expectedChangeVector.ToString();
            }

            ret[PatchKey] = RecordPatch(_patch);
            var jsonPatchIfMissing = RecordPatch(_patchIfMissing);
            if (jsonPatchIfMissing != null)
            {
                ret[PatchIfMissingKey] = jsonPatchIfMissing;
            }

            return ret;
        }

        private void RecordBool(DynamicJsonValue ret, string key, bool value)
        {
            if (value)
            {
                ret[key] = true;
            }
        }

        private DynamicJsonValue RecordPatch((PatchRequest Run, BlittableJsonReaderObject Args) patch)
        {
            if (patch.Run == null)
            {
                return null;
            }

            var ret = new DynamicJsonValue
            {
                [RunKey] = new DynamicJsonValue
                {
                    [nameof(patch.Run.Type)] = patch.Run.Type,
                    [nameof(patch.Run.Script)] = patch.Run.Script
                }
            };

            if (patch.Args != null)
            {
                ret[ArgKey] = patch.Args;
            }

            return ret;
        }

        public static PatchDocumentCommand Deserialize(BlittableJsonReaderObject mergedCmdReader, DocumentDatabase database, JsonOperationContext context)
        {

            if (!mergedCmdReader.TryGet(IdKey, out string id))
            {
                ThrowCantReadProperty(IdKey);
            }

            mergedCmdReader.TryGet(IsTestKey, out bool isTest);
            mergedCmdReader.TryGet(DebugModeKey, out bool debugMode);
            mergedCmdReader.TryGet(CollectResultsNeededKey, out bool collectResultsNeeded);
            mergedCmdReader.TryGet(SkipPatchIfChangeVectorMismatchKey, out bool skipPatchIfChangeVectorMismatch);

            var patch = ReadPatch(mergedCmdReader, PatchKey, context);
            if (patch.Item1 == null)
            {
                ThrowCantReadProperty(PatchKey);
            }
            var patchIfMissing = ReadPatch(mergedCmdReader, PatchIfMissingKey, context);

            if (!mergedCmdReader.TryGet(ExpectedChangeVectorKey, out LazyStringValue expectedChangeVector) &&
                skipPatchIfChangeVectorMismatch)
            {
                ThrowCantReadProperty(ExpectedChangeVectorKey);
            }

            var newCmd = new PatchDocumentCommand(
                context,
                id,
                expectedChangeVector,
                skipPatchIfChangeVectorMismatch,
                patch,
                patchIfMissing,
                database,
                isTest,
                debugMode,
                collectResultsNeeded);

            return newCmd;
        }

        private static (PatchRequest, BlittableJsonReaderObject) ReadPatch(BlittableJsonReaderObject mergedCmdReader, string key, JsonOperationContext context)
        {
            (PatchRequest run, BlittableJsonReaderObject arg) patch = (null, null);

            if (!mergedCmdReader.TryGet(key, out BlittableJsonReaderObject patchReader) ||
                !patchReader.TryGet(RunKey, out BlittableJsonReaderObject runReader))
            {
                return patch;
            }

            runReader.TryGet(nameof(PatchRequest.Type), out PatchRequestType type);
            runReader.TryGet(nameof(PatchRequest.Script), out string script);
            patch.run = new PatchRequest(script, type);

            if (patchReader.TryGet(ArgKey, out BlittableJsonReaderObject argReader))
            {
                patch.arg = argReader.Clone(context);
            }

            return patch;
        }

        private static void ThrowCantReadProperty(string idKey)
        {
            throw new Exception($"Can't read {idKey} of {nameof(PatchDocumentCommand)}");
        }
    }
}
