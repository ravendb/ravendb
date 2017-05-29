using System;
using Jint;
using Jint.Native;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
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

        public void Initialize()
        {
            Database.DatabaseRecordchanged += LoadCustomFunctions;
            LoadCustomFunctions();
        }

        public void Dispose()
        {
            Database.DatabaseRecordchanged -= LoadCustomFunctions;
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            engine.SetValue(PutDocument, (Func<string, JsValue, JsValue, JsValue, string>)((id, data, metadata, etag) => scope.PutDocument(id, data, metadata, etag, engine)));
            engine.SetValue(DeleteDocument, (Action<string>)scope.DeleteDocument);
        }

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
            engine.Global.Delete(PutDocument, false);
            engine.Global.Delete(DeleteDocument, false);
        }

        private void LoadCustomFunctions()
        {
            lock (_locker)
            {
                using(Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))                
                using (context.OpenReadTransaction())
                {
                    var dbrecord = Database.ServerStore.Cluster.ReadDatabase(context, Database.Name);

                    if (string.IsNullOrEmpty(dbrecord?.CustomFunctions))
                    {
                        CustomFunctions = null;
                        return;
                    }

                    CustomFunctions = dbrecord.CustomFunctions;
                }
            }
        }

        public PatchDocumentCommand GetPatchDocumentCommand(string id,
            long? etag,
            PatchRequest patch,
            PatchRequest patchIfMissing,
            bool skipPatchIfEtagMismatch,
            bool debugMode,
            bool isTest = false)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (string.IsNullOrWhiteSpace(patch.Script))
                ThrowOnEmptyPatchScript();

            if (patchIfMissing != null && string.IsNullOrWhiteSpace(patchIfMissing.Script))
                ThrowOnEmptyPatchScript(patchIfMissing: true);

            var scope = CreateOperationScope(debugMode);

            var run = CreateScriptRun(patch, scope, id); 

            var command = new PatchDocumentCommand(id, etag, skipPatchIfEtagMismatch, debugMode, scope, run, Database, Logger, isTest, patch.IsPuttingDocuments || patchIfMissing?.IsPuttingDocuments == true);

            if (patchIfMissing != null)
            {
                command.PrepareScriptRunIfDocumentMissing = () =>
                {
                    CleanupEngine(patch, run.JintEngine, scope);
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