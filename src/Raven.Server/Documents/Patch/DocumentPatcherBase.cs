using System;
using System.IO;
using System.Reflection;
using Jurassic;
using Jurassic.Compiler;
using Jurassic.Library;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public abstract class DocumentPatcherBase
    {
        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;

        protected DocumentPatcherBase()
        {
        }

        protected DocumentPatcherBase(DocumentDatabase database)
        {
            Database = database;
            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
        }

        public virtual PatchResult Apply(DocumentsOperationContext context,
            Document document,
            PatchRequest patch,
            BlittableJsonDocumentBuilder.UsageMode mode = BlittableJsonDocumentBuilder.UsageMode.None,
            IBlittableDocumentModifier modifier = null,
            bool debugMode = false)
        {
            if (document == null)
                return null;

            if (string.IsNullOrEmpty(patch.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty");

            using (var scope = CreateOperationScope(debugMode).Initialize(context))
            {
                ApplySingleScript(context, document.Id, document, patch, scope);

                var modifiedDocument = context.ReadObject(scope.ToBlittable(scope.PatchObject), document.Id, mode, modifier);

                var result = new PatchResult
                {
                    Status = PatchStatus.Patched,
                    OriginalDocument = document.Data,
                    ModifiedDocument = modifiedDocument
                };

                if (debugMode)
                {
                    throw new NotImplementedException();

                    //AddDebug(context, result, scope);
                }

                return result;
            }
        }

        protected PatcherOperationScope CreateOperationScope(bool debugMode)
        {
            throw new NotImplementedException();
        }

        protected void ApplySingleScript(DocumentsOperationContext context, string documentId, Document document, PatchRequest patch, PatcherOperationScope scope)
        {
           throw new NotImplementedException();
        }

        private void PrepareEngine(PatchRequest patch, PatcherOperationScope scope, ScriptEngine jsEngine, int documentSize)
        {
            jsEngine.SetGlobalFunction("load", (Func<string, object>)(key => scope.LoadDocument(key, jsEngine)));

            if (patch.Values != null)
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < patch.Values.Count; i++)
                {
                    patch.Values.GetPropertyByIndex(i, ref prop);
                    jsEngine.SetGlobalValue(prop.Name, scope.ToJsValue(jsEngine, prop));
                }
            }
        }

        protected abstract void CustomizeEngine(ScriptEngine engine, PatcherOperationScope scope);
    }
}
