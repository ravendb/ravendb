
using System;
using System.Collections.Generic;
using Raven.Client;
using Sparrow.Json;

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

        protected override unsafe void WriteBlittableInstance(JsHandleV8 jsObj, bool isRoot, bool filterProperties)
        {
            var obj = jsObj.AsObject() as BlittableObjectInstanceV8;
            HashSet<string> modifiedProperties = null;
            if (obj.DocumentId != null && _usageMode == BlittableJsonDocumentBuilder.UsageMode.None)
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

                    IBlittableObjectProperty<JsHandleV8> modifiedValue = default;
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

            foreach (KeyValuePair<string, BlittableObjectInstanceV8.BlittableObjectProperty> modificationKvp in obj.OwnValues)
            {
                var propertyNameAsString = modificationKvp.Key;
                //We already iterated through those properties while iterating the original properties set.
                if (modifiedProperties != null && modifiedProperties.Contains(propertyNameAsString))
                    continue;

                if (ShouldFilterProperty(filterProperties, propertyNameAsString))
                    continue;

                if (modificationKvp.Value.Changed == false)
                    continue;

                _writer.WritePropertyName(propertyNameAsString);
                IBlittableObjectProperty<JsHandleV8> blittableObjectProperty = modificationKvp.Value;
                WriteJsonValue(jsObj, isRoot, filterProperties, propertyNameAsString, blittableObjectProperty.ValueHandle);
            }
        }

        public static BlittableJsonReaderObject Translate(JsonOperationContext context, IJsEngineHandle<JsHandleV8> scriptEngine, JsHandleV8 objectInstance, IResultModifier modifier = null,
            BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, bool isRoot = true)
        {
            var blittableBridge = new JsBlittableBridgeV8(scriptEngine);
            return blittableBridge.Translate(context, objectInstance, modifier, usageMode, isRoot);
        }
    }
}
