using System;
using V8.Net;
using Sparrow.Json;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerResult : IDisposable
    {
        private readonly ScriptRunner.SingleRun _parent;

        public ScriptRunnerResult(ScriptRunner.SingleRun parent, InternalHandle instance)
        {
            _parent = parent;
            Instance = new InternalHandle(instance, true);
        }

        public readonly InternalHandle Instance;

        ~ScriptRunnerResult()
        {
            Instance.Dispose();
        }

        public InternalHandle GetOrCreate(string propertyName)
        {
            if (Instance.BoundObject != null && Instance.BoundObject is BlittableObjectInstance b)
                return b.GetOrCreate(propertyName);
            InternalHandle o = Instance.GetProperty(propertyName);
            if (o.IsUndefined || o.IsNull)
            {
                o.Dispose();
                o = _parent.ScriptEngine.CreateObject();
                Instance.SetProperty(propertyName, new InternalHandle(o, true)); // TODO error checking
            }
            return o;
        }

        public bool? BooleanValue => Instance.IsBoolean ? Instance.AsBoolean : (bool?)null;

        public bool IsNull => Instance == null || Instance.IsNull || Instance.IsUndefined;
        public string StringValue => Instance.IsStringEx() ? Instance.AsString : null;
        public InternalHandle RawJsValue => Instance;

        public BlittableJsonReaderObject TranslateToObject(JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (IsNull)
                return null;

            return JsBlittableBridge.Translate(context, _parent.ScriptEngine, Instance, modifier, usageMode);
        }

        public void Dispose()
        {
            if (Instance.BoundObject != null && Instance.BoundObject is BlittableObjectInstance boi)
                boi.Reset();

            _parent?.JavaScriptUtils.Clear();
        }
    }
}
