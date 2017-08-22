using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron.Exceptions;

namespace Raven.Server.Documents.Patch
{
    public class PatchDocumentCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
    {
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
        private readonly BlittableJsonReaderObject _patchIfMissingArgs;
        private readonly BlittableJsonReaderObject _patchArgs;

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
            bool debugMode)
        {
            _externalContext = context;
            _patchIfMissingArgs = patchIfMissing.args;
            _patchArgs = patch.args;
            _returnRun = database.Scripts.GetScriptRunner(patch.run, out _run);
            _run.DebugMode = debugMode;
            if (_runIfMissing != null)
                _runIfMissing.DebugMode = debugMode;
            _returnRunIfMissing = database.Scripts.GetScriptRunner(patchIfMissing.run, out _runIfMissing);
            _id = id;
            _expectedChangeVector = expectedChangeVector;
            _skipPatchIfChangeVectorMismatch = skipPatchIfChangeVectorMismatch;
            _database = database;
            _isTest = isTest;
            _debugMode = debugMode;
        }

        public PatchResult PatchResult { get; private set; }

        public override int Execute(DocumentsOperationContext context)
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

            object documentInstance = originalDocument;
            var args = _patchArgs;
            if (originalDocument == null)
            {
                _run = _runIfMissing;
                args = _patchIfMissingArgs;
                documentInstance = _runIfMissing.CreateEmptyObject();
            }
            var scriptResult = _run.Run(context, "execute", new[] {documentInstance, args});

            var modifiedDocument = scriptResult.Translate<BlittableJsonReaderObject>(_externalContext, 
                BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            var result = new PatchResult
            {
                Status = PatchStatus.NotModified,
                OriginalDocument = _isTest == false ? null : originalDocument?.Data?.Clone(_externalContext),
                ModifiedDocument = modifiedDocument
            };

            if (modifiedDocument == null)
            {
                result.Status = PatchStatus.Skipped;
                PatchResult = result;

                return 1;
            }

            DocumentsStorage.PutOperationResults? putResult = null;

            if (originalDocument == null)
            {
                if (_isTest == false || _run.PutOrDeleteCalled)
                    putResult = _database.DocumentsStorage.Put(context, _id, null, modifiedDocument);

                result.Status = PatchStatus.Created;
            }
            else if (DocumentCompare.IsEqualTo(originalDocument.Data, modifiedDocument,
                tryMergeAttachmentsConflict: true) == DocumentCompareResult.NotEqual)
            {
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

            if (_debugMode)
            {

                //result.Debug = 
            }

            PatchResult = result;
            return 1;
        }

        public void Dispose()
        {
            _returnRun.Dispose();
            _returnRunIfMissing.Dispose();
        }
    }
}
