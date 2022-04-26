
using System;

namespace Raven.Server.Documents.Patch.V8
{

    public class JsBlittableBridgeV8 : JsBlittableBridge<JsHandleV8>
    {
        [ThreadStatic]
        private static uint _recursiveNativeObjectsCount;

        public JsBlittableBridgeV8(IJsEngineHandle<JsHandleV8> scriptEngine) : base(scriptEngine)
        {
        }

        public override void WriteValueInternal(object target, JsHandleV8 jsObj, bool filterProperties)
        {
            var isNativeObj = jsObj.IsObject && jsObj.Item.ObjectID < 0;
            if (isNativeObj)
            {
                _recursiveNativeObjectsCount += 1;
            }

            try
            {
                if (isNativeObj && _recursiveNativeObjectsCount > 1000)
                    _writer.WriteValueNull();
                else if (_recursive.Add(target))
                    WriteInstance(jsObj, modifier: null, isRoot: false, filterProperties: filterProperties);
                else
                    _writer.WriteValueNull();
            }
            finally
            {
                if (isNativeObj && _recursiveNativeObjectsCount > 0)
                {
                    _recursiveNativeObjectsCount -= 1;
                }
                _recursive.Remove(target);
            }
        }

    }
}
