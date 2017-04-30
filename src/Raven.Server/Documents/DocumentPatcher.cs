using System;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents
{
    public sealed class DocumentPatcher : DocumentPatcherBase, IDisposable
    {
        private readonly object _locker = new object();

        public DocumentPatcher(DocumentDatabase database)
            : base(database)
        {
        }

        public string CustomFunctions { get; private set; }

        public void Initialize()
        {
            Database.Changes.OnSystemDocumentChange += HandleDocumentChange;
            LoadCustomFunctions();
        }

        public void Dispose()
        {
            Database.Changes.OnSystemDocumentChange -= HandleDocumentChange;
        }

        private void HandleDocumentChange(DocumentChange change)
        {
            if (change.IsSystemDocument == false)
                return;

            if (change.Type != DocumentChangeTypes.Put && change.Type != DocumentChangeTypes.Delete)
                return;

            if (string.Equals(change.Key, Constants.Json.CustomFunctionsKey, StringComparison.OrdinalIgnoreCase) == false)
                return;

            if (change.Type == DocumentChangeTypes.Delete)
            {
                CustomFunctions = null;
                return;
            }

            LoadCustomFunctions();
        }

        private void LoadCustomFunctions()
        {
            lock (_locker)
            {
                DocumentsOperationContext context;
                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var json = Database.DocumentsStorage.Get(context, Constants.Json.CustomFunctionsKey);

                    string functions;
                    if (json == null || json.Data.TryGet("Functions", out functions) == false || string.IsNullOrWhiteSpace(functions))
                    {
                        CustomFunctions = null;
                        return;
                    }

                    CustomFunctions = functions;
                }
            }
        }
    }
}