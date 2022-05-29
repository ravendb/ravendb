using System.Collections;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Runtime;
using Jint.Runtime.Interop;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch.Jint;

public class JsBlittableBridgeJint : JsBlittableBridge<JsHandleJint>
{
    public JsBlittableBridgeJint(IJsEngineHandle<JsHandleJint> scriptEngine) : base(scriptEngine)
    {
    }

    protected override void WriteNestedObject(JsHandleJint jsObj, bool filterProperties)
    {
        if (_recursive == null)
            _recursive = new HashSet<object>();

        var obj = jsObj.Item;
        if (obj is ObjectWrapper objectWrapper)
        {
            var target = objectWrapper.Target;

            if (target is IDictionary)
            {
                WriteValueInternal(target, jsObj, filterProperties);
            }
            else if (target is IEnumerable enumerable)
            {
                _writer.StartWriteArray();
                int i = 0;
                foreach (var item in enumerable)
                {
                    using (var jsItem = _scriptEngine.FromObjectGen(item))
                    {
                        WriteJsonValue(jsItem, false, filterProperties, i.ToString(), jsItem);
                    }
                    i++;
                }
                _writer.WriteArrayEnd();
            }
            else
                WriteObjectType(target);
        }
        else if (obj is FunctionInstance)
            _writer.WriteValueNull();
        else
            WriteValueInternal(jsObj, jsObj, filterProperties);
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

    protected override unsafe void WriteBlittableInstance(JsHandleJint jsObj, bool isRoot, bool filterProperties)
    {
        var obj = jsObj.AsObject() as BlittableObjectInstanceJint;
        HashSet<string> modifiedProperties = null;
        if (obj.DocumentId != null &&
            _usageMode == BlittableJsonDocumentBuilder.UsageMode.None)
        {
            var metadata = obj.GetOrCreate(Constants.Documents.Metadata.Key);
            if (metadata.AsObject() is IBlittableObjectInstance boi)
            {
                using (var jsDocId = _scriptEngine.CreateValue(obj.DocumentId))
                    obj.SetOwnProperty(Constants.Documents.Metadata.Id, jsDocId, toReturnCopy: false);
            }
            else
            {
                metadata.SetProperty(Constants.Documents.Metadata.Id, _scriptEngine.CreateValue(obj.DocumentId));
            }
        }

        if (obj.Blittable != null)
        {
            using var propertiesByInsertionOrder = obj.Blittable.GetPropertiesByInsertionOrder();
            for (int i = 0; i < propertiesByInsertionOrder.Size; i++)
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();
                var propIndex = propertiesByInsertionOrder.Properties[i];
                obj.Blittable.GetPropertyByIndex(propIndex, ref prop);

                IBlittableObjectProperty<JsHandleJint> modifiedValue = default;
                var key = prop.Name.ToString();
                var existInObject = obj.TryGetValue(key, out modifiedValue, out bool isDeleted);

                if (isDeleted)
                    continue;

                if (existInObject)
                {
                    modifiedProperties ??= new HashSet<string>();

                    modifiedProperties.Add(prop.Name);
                }

                if (ShouldFilterProperty(filterProperties, prop.Name))
                    continue;

                _writer.WritePropertyName(prop.Name);

                if (existInObject && modifiedValue.Changed)
                {
                    WriteJsonValue(jsObj, isRoot, filterProperties, prop.Name, modifiedValue.ValueHandle);
                }
                else
                {
                    _writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }
        }

        if (obj.OwnValues == null)
            return;

        foreach (KeyValuePair<JsValue, BlittableObjectInstanceJint.BlittableObjectProperty> modificationKvp in obj.OwnValues)
        {
            var propertyNameAsString = modificationKvp.Key.AsString();
            //We already iterated through those properties while iterating the original properties set.
            if (modifiedProperties != null && modifiedProperties.Contains(propertyNameAsString))
                continue;

            if (ShouldFilterProperty(filterProperties, propertyNameAsString))
                continue;

            if (modificationKvp.Value.Changed == false)
                continue;

            _writer.WritePropertyName(propertyNameAsString);
            IBlittableObjectProperty<JsHandleJint> blittableObjectProperty = modificationKvp.Value;
            WriteJsonValue(jsObj, isRoot, filterProperties, propertyNameAsString, blittableObjectProperty.ValueHandle);
        }
    }

    public static BlittableJsonReaderObject Translate(JsonOperationContext context, IJsEngineHandle<JsHandleJint> scriptEngine, JsHandleJint objectInstance, IResultModifier modifier = null, 
        BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, bool isRoot = true)
    {
        var blittableBridge = new JsBlittableBridgeJint(scriptEngine);
        return blittableBridge.Translate(context, objectInstance, modifier, usageMode, isRoot);
    }
}
