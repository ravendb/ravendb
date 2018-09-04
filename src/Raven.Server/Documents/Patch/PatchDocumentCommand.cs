using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Jint.Native;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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
            bool debugMode,
            bool collectResultsNeeded)
        {
            _externalContext = collectResultsNeeded ? context : null;
            _patchIfMissingArgs = patchIfMissing.args;
            _patchArgs = patch.args;
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

            object documentInstance;
            var args = _patchArgs;
            BlittableJsonReaderObject originalDoc;
            if (originalDocument == null)
            {
                _run = _runIfMissing;
                args = _patchIfMissingArgs;
                documentInstance = _runIfMissing.CreateEmptyObject();
                originalDoc = null;
            }
            else
            {
                documentInstance = UpdateOriginalDocument();
            }

            // we will to acccess this value, and the original document data may be changed by
            // the actions of the script, so we translate (which will create a clone) then use
            // that clone later
            using (var scriptResult = _run.Run(context, context, "execute", _id, new[] { documentInstance, args }))
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

                if (_run.RefreshOriginalDocument)
                {
                    originalDocument = _database.DocumentsStorage.Get(context, _id);
                    documentInstance = UpdateOriginalDocument();
                }

                if (_run.UpdatedDocumentCounterIds != null)
                {
                    foreach (var docId in _run.UpdatedDocumentCounterIds)
                    {
                        if (docId.Equals(_id, StringComparison.OrdinalIgnoreCase))
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
                    if (_isTest == false || _run.PutOrDeleteCalled)
                        putResult = _database.DocumentsStorage.Put(context, _id, null, modifiedDocument);

                    result.Status = PatchStatus.Created;
                }
                else if (DocumentCompare.IsEqualTo(originalDoc, modifiedDocument,
                    tryMergeMetadataConflicts: true) != DocumentCompareResult.Equal)
                {
                    Debug.Assert(originalDocument != null);
                    if (_isTest == false || _run.PutOrDeleteCalled)
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

                PatchResult = result;
                return 1;
            }

            BlittableObjectInstance UpdateOriginalDocument()
            {
                originalDoc = null;

                if (originalDocument != null)
                {
                    var translated = (BlittableObjectInstance)((JsValue)_run.Translate(context, originalDocument)).AsObject();
                    // here we need to use the _cloned_ version of the document, since the patch may
                    // change it
                    originalDoc = translated.Blittable;
                    originalDocument.Data = null; // prevent access to this by accident

                    return translated;
                }

                return null;
            }
        }

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
                {[Constants.Documents.Metadata.Key] = metadata};

            modifiedDocument = context.ReadObject(modifiedDocument, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            return modifiedDocument;
        }

        public void Dispose()
        {
            _returnRun.Dispose();
            _returnRunIfMissing.Dispose();
        }
    }
}
