using System;
using Sparrow.Json;
using Raven.Client.ServerWide.JavaScript;
using PatchJint = Raven.Server.Documents.Patch.Jint;
using PatchV8 = Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerResult : IDisposable
    {
        private readonly JavaScriptEngineType _jsEngineType; 
        
        private readonly ScriptRunner.SingleRun _parent;
        public JsHandle Instance;
        
        public ScriptRunnerResult(ScriptRunner.SingleRun parent, JsHandle instance)
        {
            _parent = parent;
            Instance = instance.Clone();
            _jsEngineType = Instance.EngineType;
        }

        ~ScriptRunnerResult()
        {
            Instance.Dispose();
        }

        public IJsEngineHandle EngineHandle => _parent.ScriptEngineHandle;

        public JsHandle GetOrCreate(string propertyName)
        {
            if (Instance.Object is IBlittableObjectInstance boi)
                return boi.GetOrCreate(propertyName);

            JsHandle o = Instance.GetProperty(propertyName);
            if (o.IsUndefined || o.IsNull)
            {
                o.Dispose();
                o = _parent.ScriptEngineHandle.CreateObject();
                Instance.SetProperty(propertyName, new JsHandle(ref o), throwOnError: true);
            }
            return o;
        }

        public bool? BooleanValue => Instance.IsBoolean ? Instance.AsBoolean : (bool?)null;

        public bool IsNull => Instance.IsEmpty || Instance.IsNull || Instance.IsUndefined;
        public string StringValue => Instance.IsStringEx ? Instance.AsString : null;
        public JsHandle RawJsValue => Instance;

        public BlittableJsonReaderObject TranslateToObject(JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (IsNull)
                return null;

            return _jsEngineType switch
            {
                JavaScriptEngineType.Jint => PatchJint.JsBlittableBridgeJint.Translate(context, _parent.ScriptEngineJint, Instance.Jint.Obj, modifier, usageMode),
                JavaScriptEngineType.V8 => PatchV8.JsBlittableBridgeV8.Translate(context, _parent.ScriptEngineV8, Instance.V8.Item, modifier, usageMode),
                _ => throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.")
            };
        }

        public void Dispose()
        {
            if (Instance.IsObject)
            {
                var io = Instance.Object;
                if (io is PatchJint.BlittableObjectInstanceJint boiJint)
                    boiJint.Reset();
                else if (io is PatchV8.BlittableObjectInstanceV8 boiV8)
                    boiV8.Reset();
            }

            _parent?.JsUtilsBase.Clear();
        }
        
    }
}
