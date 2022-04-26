namespace Raven.Server.Documents.Patch.Jint;

public class JsBlittableBridgeJint : JsBlittableBridge<JsHandleJint>
{
    public JsBlittableBridgeJint(IJsEngineHandle<JsHandleJint> scriptEngine) : base(scriptEngine)
    {
    }

    public override void WriteValueInternal(object target, JsHandleJint jsObj, bool filterProperties)
    {
        try
        {
            if (_recursive.Add(target))
                WriteInstance(jsObj, modifier: null, isRoot: false, filterProperties: filterProperties);
            else
                _writer.WriteValueNull();
        }
        finally
        {
            _recursive.Remove(target);
        }
    }
}
