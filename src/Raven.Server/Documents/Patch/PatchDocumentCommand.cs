using System;
using Jurassic.Library;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron.Exceptions;

namespace Raven.Server.Documents.Patch
{
    public class PatchDocumentCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly string _id;
        private readonly LazyStringValue _expectedChangeVector;
        private readonly bool _skipPatchIfChangeVectorMismatch;
        private readonly bool _debugMode;

        private readonly JsonOperationContext _externalContext;
        private readonly PatcherOperationScope _scope;

        private readonly DocumentDatabase _database;
        private readonly Logger _logger;
        private readonly bool _isTest;
        private readonly bool _scriptIsPuttingDocument;

        private DocumentPatcherBase.SingleScriptRun _run;

        public PatchDocumentCommand(JsonOperationContext context, string id, LazyStringValue expectedChangeVector, bool skipPatchIfChangeVectorMismatch, bool debugMode, PatcherOperationScope scope, DocumentPatcherBase.SingleScriptRun run, DocumentDatabase database, Logger logger, bool isTest, bool scriptIsPuttingDocument)
        {
            _externalContext = context;
            _scope = scope;
            _run = run;
            _id = id;
            _expectedChangeVector = expectedChangeVector;
            _skipPatchIfChangeVectorMismatch = skipPatchIfChangeVectorMismatch;
            _debugMode = debugMode;
            _database = database;
            _logger = logger;
            _isTest = isTest;
            _scriptIsPuttingDocument = scriptIsPuttingDocument;
        }

        public PatchResult PatchResult { get; private set; }

        public Func<DocumentPatcherBase.SingleScriptRun> PrepareScriptRunIfDocumentMissing { get; set; }

        public override int Execute(DocumentsOperationContext context)
        {
            var originalDocument = _database.DocumentsStorage.Get(context, _id);

            if (_expectedChangeVector != null)
            {
                if (originalDocument == null)
                {
                    if (_skipPatchIfChangeVectorMismatch)
                    {
                        PatchResult = new PatchResult { Status = PatchStatus.Skipped };
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
                        PatchResult = new PatchResult { Status = PatchStatus.Skipped };
                        return 1;
                    }

                    throw new ConcurrencyException($"Could not patch document '{_id}' because non current change vector was used")
                    {
                        ActualChangeVector = originalDocument.ChangeVector,
                        ExpectedChangeVector = _expectedChangeVector
                    };
                }
            }

            if (originalDocument == null && PrepareScriptRunIfDocumentMissing == null)
            {
                PatchResult = new PatchResult { Status = PatchStatus.DocumentDoesNotExist };
                return 1;
            }

            var document = originalDocument;

            if (originalDocument == null)
            {
                _run = PrepareScriptRunIfDocumentMissing();

                var djv = new DynamicJsonValue { [Constants.Documents.Metadata.Key] = { } };
                var data = context.ReadObject(djv, _id);
                document = new Document { Data = data };
            }

            _scope.Initialize(context);

            try
            {
                var keeper = _run.JSEngine.OnLoopIterationCallTarget as DocumentPatcherBase.EngineLoopIterationKeeper;
                if (keeper == null)
                {
                    _run.JSEngine.OnLoopIterationCall = new DocumentPatcherBase.EngineLoopIterationKeeper(document.Data.Size).OnLoopIteration;
                }
                else
                {
                    keeper.MaxLoopIterations = document.Data.Size;
                }

                

                _scope.PatchObject = _scope.ToJsObject(_run.JSEngine, document);

                _run.Execute();
            }
            catch (Exception errorEx)
            {
                _run.HandleError(errorEx);
                throw;
            }

            var modifiedDocument = _externalContext.ReadObject(_scope.ToBlittable(_scope.PatchObject as ObjectInstance), document.Id,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk, new BlittableMetadataModifier(_externalContext));

            var result = new PatchResult
            {
                Status = PatchStatus.NotModified,
                OriginalDocument = _isTest == false ? originalDocument?.Data : originalDocument?.Data?.Clone(_externalContext),
                ModifiedDocument = modifiedDocument
            };

            if (_debugMode)
                DocumentPatcherBase.AddDebug(_externalContext, result, _scope);

            if (modifiedDocument == null)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"After applying patch, modifiedDocument is null and document is null? {originalDocument == null}");

                result.Status = PatchStatus.Skipped;
                PatchResult = result;

                return 1;
            }

            DocumentsStorage.PutOperationResults? putResult = null;

            if (originalDocument == null)
            {
                if (_isTest == false || _scriptIsPuttingDocument)
                    putResult = _database.DocumentsStorage.Put(context, _id, null, modifiedDocument);

                result.Status = PatchStatus.Created;
            }
            else if (DocumentCompare.IsEqualTo(originalDocument.Data, modifiedDocument, tryMergeAttachmentsConflict: true) == DocumentCompareResult.NotEqual)
            {
                if (_isTest == false || _scriptIsPuttingDocument)
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
}
