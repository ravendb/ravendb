using System;
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

        public void Set(string name, string value)
        {
            ((BlittableObjectInstance)_instance.AsObject()).Put(name, new JsValue(value), false);
        }

        public ObjectInstance GetOrCreate(string name)
        {
            if (_instance.AsObject() is BlittableObjectInstance boi)
                return boi.GetOrCreate(name);
            var parent = _instance.AsObject();
            var o = parent.Get(name);
            if (o == null)
            {
                o = _parent.ScriptEngine.Object.Create(_parent.ScriptEngine.Object.PrototypeObject,
                    Array.Empty<JsValue>());
                parent.Put(name, o, false);
            }
            return o.AsObject();
        }

        public object Value => _instance;
        public bool IsNull => _instance == null || _instance.IsNull();

        public BlittableJsonReaderObject Translate(JsonOperationContext context,
            BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (IsNull || _instance.IsNull())
                return null;

            var obj = _instance.AsObject();
            return JsBlittableBridge.Translate(context, obj, usageMode);
        }

        public void Dispose()
        {
            _parent?.DisposeClonedDocuments();
        }
    }
}
