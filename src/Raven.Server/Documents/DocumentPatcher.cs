using System;
using Jurassic;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Patch;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents
{
    public sealed class DocumentPatcher : DocumentPatcherBase, IDisposable
    {
        internal const string PutDocument = "PutDocument";
        internal const string DeleteDocument = "DeleteDocument";

        private readonly object _locker = new object();

        public DocumentPatcher(DocumentDatabase database)
            : base(database)
        {
        }

        public string CustomFunctions { get; private set; }

        public void Initialize(DatabaseRecord record)
        {
            Database.DatabaseRecordChanged += LoadCustomFunctions;
            LoadCustomFunctions(record);
        }

        public void Dispose()
        {
            Database.DatabaseRecordChanged -= LoadCustomFunctions;
        }

        protected override void CustomizeEngine(ScriptEngine engine, PatcherOperationScope scope)
        {
            engine.SetGlobalFunction(PutDocument, (Func<string, object, object, string, string>)((id, data, metadata, changeVector) => scope.PutDocument(id, data, metadata, changeVector, engine)));

            engine.SetGlobalFunction(DeleteDocument, (Action<string>)scope.DeleteDocument);
        }

        protected override void RemoveEngineCustomizations(ScriptEngine engine, PatcherOperationScope scope)
        {
            engine.Global.Delete(PutDocument, false);
            engine.Global.Delete(DeleteDocument, false);
        }

        private void LoadCustomFunctions(DatabaseRecord dbrecord)
        {
            lock (_locker)
            {
                if (string.IsNullOrEmpty(dbrecord?.CustomFunctions))
                {
                    CustomFunctions = null;
                    return;
                }

                CustomFunctions = dbrecord.CustomFunctions;
            }
        }

        public PatchDocumentCommand GetPatchDocumentCommand(
            JsonOperationContext context, string id, LazyStringValue changeVector, 
            PatchRequest patch, PatchRequest patchIfMissing, bool skipPatchIfChangeVectorMismatch, bool debugMode, bool isTest = false)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (string.IsNullOrWhiteSpace(patch.Script))
                ThrowOnEmptyPatchScript();

            if (patchIfMissing != null && string.IsNullOrWhiteSpace(patchIfMissing.Script))
                ThrowOnEmptyPatchScript(patchIfMissing: true);

            var scope = CreateOperationScope(debugMode);

            var run = CreateScriptRun(patch, scope, id); 

            var command = new PatchDocumentCommand(context, id, changeVector, skipPatchIfChangeVectorMismatch, debugMode, scope, run, Database, Logger, isTest, patch.IsPuttingDocuments || patchIfMissing?.IsPuttingDocuments == true);

            if (patchIfMissing != null)
            {
                command.PrepareScriptRunIfDocumentMissing = () =>
                {
                    CleanupEngine(patch, run.JSEngine, scope);
                    return CreateScriptRun(patchIfMissing, scope, id);
                };
            }

            return command;
        }

        private static void ThrowOnEmptyPatchScript(bool patchIfMissing = false)
        {
            throw new InvalidOperationException($"{(patchIfMissing == false ? nameof(PatchCommandData.Patch) : nameof(PatchCommandData.PatchIfMissing))} " +
                                                "script must be non-null and not empty.");
        }

        private SingleScriptRun CreateScriptRun(PatchRequest patch, PatcherOperationScope scope, string id)
        {
            var run = new SingleScriptRun(this, patch, scope);

            run.Prepare(0);
            run.SetDocumentId(id);

            return run;
        }
    }
}
