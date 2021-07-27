using System;
using V8.Net;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerResult : IDisposable
    {
        private readonly ScriptRunner.SingleRun _parent;

        public ScriptRunnerResult(ScriptRunner.SingleRun parent, InternalHandle instance) : base(parent)
        {
            _parent = parent;
            Instance.Set(instance);
        }

        public readonly InternalHandle Instance;

        ~ScriptRunnerResult()
        {
            Instance.Dispose();
        }

        public V8NativeObject GetOrCreate(string propertyName)
        {
            using (propertyName);
            
            if (Instance.IsObject && Instance.BoundObject is BlittableObjectInstance b)
                return b.GetOrCreate(propertyName);
            var parent = Instance.Object;

            using (var o = parent.GetProperty(propertyName))
            {
                if (o.IsUndefined || o.IsNull)
                {
                    o.Set(_parent.ScriptEngine.CreateObject());
                    parent.SetProperty(propertyName, o);
                }
                return o.Object; // no need to KeepTrack() as we return Handle
            }
        }

        public bool? BooleanValue => Instance.IsBoolean ? Instance.AsBoolean : (bool?)null;

        public bool IsNull => Instance == null || Instance.IsNull || Instance.IsUndefined;
        public string StringValue => Instance.IsString ? Instance.AsString : null;
        public InternalHandle RawJsValue => Instance;

        public BlittableJsonReaderObject TranslateToObject(JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (IsNull)
                return null;

            return JsBlittableBridge.Translate(context, _parent.ScriptEngine, Instance.Object, modifier, usageMode);
        }

        public void Dispose()
        {
            if (Instance is BlittableObjectInstance boi)
                boi.Reset();

            _parent?.JavaScriptUtils.Clear();
        }
    }
}
