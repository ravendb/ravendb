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

        public ScriptRunnerResult(ScriptRunner.SingleRun parent, JsValue instance)
        {
            _parent = parent;
            Instance = instance;
        }

        public readonly JsValue Instance;

        public ObjectInstance GetOrCreate(in Key name)
        {
            if (Instance.AsObject() is BlittableObjectInstance b)
                return b.GetOrCreate(name);
            var parent = Instance.AsObject();
            var o = parent.Get(name);
            if (o == null || o.IsUndefined() || o.IsNull())
            {
                o = _parent.ScriptEngine.Object.Construct(Array.Empty<JsValue>());
                parent.Set(name, o, false);
            }
            return o.AsObject();
        }

        public bool? BooleanValue => Instance.IsBoolean() ? Instance.AsBoolean() : (bool?)null;

        public bool IsNull => Instance == null || Instance.IsNull() || Instance.IsUndefined();
        public string StringValue => Instance.IsString() ? Instance.AsString() : null;
        public JsValue RawJsValue => Instance;

        public BlittableJsonReaderObject TranslateToObject(JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (IsNull)
                return null;

            var obj = Instance.AsObject();
            return JsBlittableBridge.Translate(context, _parent.ScriptEngine, obj, modifier, usageMode);
        }

        public void Dispose()
        {
            _parent?.JavaScriptUtils.Clear();
        }
    }
}
