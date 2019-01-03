using System;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerResult : IDisposable
    {
        private readonly ScriptRunner.SingleRun _parent;
        private readonly JsValue _instance;

        public ScriptRunnerResult(ScriptRunner.SingleRun parent, JsValue instance)
        {
            _parent = parent;
            _instance = instance;
        }

        public JsValue Instance => _instance;

        public void Set(string name, string value)
        {
            ((BlittableObjectInstance)_instance.AsObject()).Put(name, value, false);
        }

        public ObjectInstance GetOrCreate(string name)
        {
            if (_instance.AsObject() is BlittableObjectInstance b)
                return b.GetOrCreate(name);
            var parent = _instance.AsObject();
            var o = parent.Get(name);
            if (o == null || o.IsUndefined() || o.IsNull())
            {
                o = _parent.ScriptEngine.Object.Construct(Array.Empty<JsValue>());
                parent.Put(name, o, false);
            }
            return o.AsObject();
        }

        public bool? BooleanValue => _instance.IsBoolean() ? _instance.AsBoolean() : (bool?)null;

        public bool IsNull => _instance == null || _instance.IsNull() || _instance.IsUndefined();
        public string StringValue => _instance.IsString() ? _instance.AsString() : null;
        public JsValue RawJsValue => _instance;

        public BlittableJsonReaderObject TranslateToObject(JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (IsNull)
                return null;

            var obj = _instance.AsObject();
            return JsBlittableBridge.Translate(context, _parent.ScriptEngine, obj, modifier, usageMode);
        }

        public void Dispose()
        {            
            _parent?.DisposeClonedDocuments();
        }
    }
}
