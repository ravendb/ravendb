using System;
using Jint;
using Jint.Native;
using Raven.Abstractions.Data;

namespace Raven.Server.Documents.Patch
{
    public class PatcherOperationScope : IDisposable
    {
        public bool DebugMode { get; }

        public string CustomFunctions { get; set; }

        public int AdditionalStepsPerSize { get; set; }
        public int MaxSteps { get; set; }

        public JsValue ActualPatchResult { get; set; }

        public PatcherOperationScope(bool debugMode = false)
        {
            DebugMode = debugMode;
        }

        public JsValue LoadDocument(string documentKey, Engine engine, ref int totalStatements)
        {
            throw new NotImplementedException();
        }

        public string PutDocument(string documentKey, object data, object metadata, Engine jintEngine)
        {
            throw new NotImplementedException();
        }

        public void DeleteDocument(string documentKey)
        {
            throw new NotImplementedException();
        }

        public JsValue ToJsInstance(Engine engine, object value, string propertyKey = null)
        {
            if (value == null)
                return JsValue.Null;

            throw new NotImplementedException();
        }

        public JsValue ToJsObject(Engine engine, Document document, string propertyName = null)
        {
            throw new NotImplementedException();
        }

        public virtual object ConvertReturnValue(JsValue jsObject)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            
        }
    }
}