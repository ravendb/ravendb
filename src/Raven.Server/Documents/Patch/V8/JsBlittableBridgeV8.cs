using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using V8.Net;
using Raven.Server.Extensions.V8;
using Raven.Client;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch.V8
{
    public class JsBlittableBridgeV8 : JsBlittableBridge
    {
        private readonly V8EngineEx _engine;

        static JsBlittableBridgeV8()
        {
        }

        public JsBlittableBridgeV8(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, BlittableJsonDocumentBuilder.UsageMode usageMode, V8EngineEx engine) 
            : base(writer, usageMode)
        {
            _engine = engine;
        }

        private void WriteInstance(InternalHandle jsObj, IResultModifier modifier, bool isRoot, bool filterProperties)
        {
            _writer.StartWriteObject();

            modifier?.Modify(new JsHandle(jsObj));

            object boundObject = jsObj.BoundObject;
            if (boundObject is BlittableObjectInstanceV8 blittableJsObject)
                WriteBlittableInstance(jsObj, isRoot, filterProperties);
            else
                WriteJsInstance(jsObj, isRoot, filterProperties);

            _writer.WriteObjectEnd();
        }

        private void WriteJsonValue(InternalHandle jsParent, bool isRoot, bool filterProperties, string propertyName, InternalHandle jsValue)
        {
            jsValue.ThrowOnError();

            if (jsValue.IsBoolean)
                _writer.WriteValue(jsValue.AsBoolean);
            else if (jsValue.IsUndefined || jsValue.IsNull)
                _writer.WriteValueNull();
            else if (jsValue.IsStringEx)
                _writer.WriteValue(jsValue.AsString);
            else if (jsValue.IsDate)
            {
                var primitiveValue = jsValue.AsDouble;
                if (double.IsNaN(primitiveValue) ||
                    primitiveValue > MaxJsDateMs ||
                    primitiveValue < MinJsDateMs)
                    // not a valid Date. 'ToDateTime()' will throw
                    throw new InvalidOperationException($"Invalid 'DateInstance' on property '{propertyName}'. Date value : '{primitiveValue}'. " +
                                                        "Note that JavaScripts 'Date' measures time as the number of milliseconds that have passed since the Unix epoch.");

                var date = jsValue.AsDate;
                _writer.WriteValue(date.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
            }
            else if (jsValue.IsInt32)
                WriteNumber(jsParent.BoundObject, propertyName, jsValue.AsInt32);
            else if (jsValue.IsNumberEx)
                WriteNumber(jsParent.BoundObject, propertyName, jsValue.AsDouble);
            else if (jsValue.IsArray)
                WriteArray(jsValue);
            else if (jsValue.IsObject)
            {
                if (isRoot)
                    filterProperties = string.Equals(propertyName, Constants.Documents.Metadata.Key, StringComparison.Ordinal);

                object target = jsValue.BoundObject;
                if (target != null)
                {
                    if (target is LazyNumberValue)
                    {
                        _writer.WriteValue(BlittableJsonToken.LazyNumber, target);
                    }
                    else if (target is LazyStringValue)
                    {
                        _writer.WriteValue(BlittableJsonToken.String, target);
                    }
                    else if (target is LazyCompressedStringValue)
                    {
                        _writer.WriteValue(BlittableJsonToken.CompressedString, target);
                    }
                    else if (target is long)
                    {
                        _writer.WriteValue(BlittableJsonToken.Integer, (long)target);
                    }
                    else
                    {
                        WriteNestedObject(jsValue, filterProperties);
                    }
                }
                else
                {
                    WriteNestedObject(jsValue, filterProperties);
                }
            }
        }

        private void WriteJsonValue(object parent, bool isRoot, bool filterProperties, string propertyName, object value)
        {
            if (value is InternalHandle jsValue)
            {
                WriteJsonValue((InternalHandle)parent, isRoot, filterProperties, propertyName, jsValue);
                return;
            }
            WriteValue(parent, isRoot, propertyName, value);
        }

        private void WriteArray(InternalHandle jsArr)
        {
            _writer.StartWriteArray();
            for (int i = 0; i < jsArr.ArrayLength; i++)
            {
                using (var jsValue = jsArr.GetProperty(i))
                {
                    WriteJsonValue(jsArr, false, false, i.ToString(), jsValue);
                }
            }
            _writer.WriteArrayEnd();
        }

        private void WriteValue(object parent, bool isRoot, string propertyName, InternalHandle jsValue)
        {
            if (jsValue.IsUndefined || jsValue.IsNull)
                _writer.WriteValueNull();
            else if (jsValue.IsArray)
            {
                //WriteArray(jsArr._); // seems to be simpler, except for it calls WriteJsonValue - but there should be no difference
                _writer.StartWriteArray();
                for (int i = 0; i < jsValue.ArrayLength; i++)
                {
                    using (var jsItem = jsValue.GetProperty(i))
                    {
                        WriteValue(jsValue, false, i.ToString(), jsItem);
                    }
                }
                _writer.WriteArrayEnd();
            }
            else if (jsValue.IsObject)
            {
                if (jsValue.IsRegExp)
                    _writer.WriteValueNull();
                else
                {
                    var filterProperties = isRoot && string.Equals(propertyName, Constants.Documents.Metadata.Key, StringComparison.Ordinal);
                    WriteNestedObject(jsValue, filterProperties);
                }
            }
        }

        private void WriteValue(object parent, bool isRoot, string propertyName, object value)
        {
            if (value is bool b)
                _writer.WriteValue(b);
            else if (value is string s)
                _writer.WriteValue(s);
            else if (value is byte by)
                _writer.WriteValue(by);
            else if (value is int n)
                WriteNumber(parent, propertyName, n);
            else if (value is uint ui)
                _writer.WriteValue(ui);
            else if (value is long l)
                _writer.WriteValue(l);
            else if (value is double d)
            {
                WriteNumber(parent, propertyName, d);
            }
            else if (value == null)
                _writer.WriteValueNull();
            else if (value is LazyStringValue lsv)
            {
                _writer.WriteValue(lsv);
            }
            else if (value is LazyCompressedStringValue lcsv)
            {
                _writer.WriteValue(lcsv);
            }
            else if (value is LazyNumberValue lnv)
            {
                _writer.WriteValue(lnv);
            }
            else
            {
                throw new NotSupportedException(value.GetType().ToString());
            }
        }

        private void WriteNestedObject(InternalHandle jsObj, bool filterProperties)
        {
            if (_recursive == null)
                _recursive = new HashSet<object>();

            var target = jsObj.BoundObject;
            if (target != null)
            {
                if (target is IDictionary || target is BlittableObjectInstanceV8 blittableJsObject)
                {
                    WriteValueInternal(target, jsObj, filterProperties);
                }
                else if (target is IEnumerable enumerable)
                {
                    _writer.StartWriteArray();
                    int i = 0;
                    foreach (var item in enumerable)
                    {
                        using (var jsItem = _engine.FromObject(item))
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
            else if (jsObj.IsFunction)
                _writer.WriteValueNull();
            else
                WriteValueInternal(jsObj.HandleID, jsObj, filterProperties);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteValueInternal(object target, InternalHandle jsObj, bool filterProperties)
        {
            var isNativeObj = jsObj.IsObject && jsObj.ObjectID < 0;
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

        private void WriteNumber(object parent, string propName, double d)
        {
            var writer = _writer;
            var boi = parent as BlittableObjectInstanceV8;
            if (boi == null || propName == null)
            {
                GuessNumberType();
                return;
            }

            if (boi.OriginalPropertiesTypes != null &&
                boi.OriginalPropertiesTypes.TryGetValue(propName, out var numType))
            {
                if (WriteNumberBasedOnType(numType & BlittableJsonReaderBase.TypesMask))
                    return;
            }
            else if (boi.Blittable != null)
            {
                var propIndex = boi.Blittable.GetPropertyIndex(propName);
                if (propIndex != -1)
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();
                    boi.Blittable.GetPropertyByIndex(propIndex, ref prop);
                    if (WriteNumberBasedOnType(prop.Token & BlittableJsonReaderBase.TypesMask))
                        return;
                }
            }

            GuessNumberType();

            bool WriteNumberBasedOnType(BlittableJsonToken type)
            {
                if (type == BlittableJsonToken.LazyNumber)
                {
                    writer.WriteValue(d);
                    return true;
                }

                if (type == BlittableJsonToken.Integer)
                {
                    if (IsDoubleType())
                    {
                        // the previous value was a long and now changed to double
                        writer.WriteValue(d);
                    }
                    else
                    {
                        writer.WriteValue((long)d);
                    }

                    return true;
                }

                return false;
            }

            void GuessNumberType()
            {
                if (IsDoubleType())
                {
                    writer.WriteValue(d);
                }
                else
                {
                    writer.WriteValue((long)d);
                }
            }

            bool IsDoubleType()
            {
                var roundedNumber = Math.Round(d, 0);
                if (roundedNumber.AlmostEquals(d))
                {
                    var digitsAfterDecimalPoint = Math.Abs(roundedNumber - d);
                    if (digitsAfterDecimalPoint == 0 && Math.Abs(roundedNumber) <= long.MaxValue)
                        return false;
                }

                return true;
            }
        }

        private void WriteJsInstance(InternalHandle jsObj, bool isRoot, bool filterProperties)
        {
            var properties = jsObj.IsBinder ? GetBoundObjectProperties(jsObj) : jsObj.GetOwnProperties(); // TODO [shomo] for most used bound classes (BOI, Attachment, Counter, TimeSeries) we could use the optimized GetBoundObjectProperties version with direct getting of properties without conversions to Js format and back
            foreach (var (propertyName, jsPropertyValue) in properties)
            {
                using (jsPropertyValue)
                {
                    if (ShouldFilterProperty(filterProperties, propertyName))
                        continue;

                    if (jsPropertyValue.IsEmpty)
                        continue;

                    _writer.WritePropertyName(propertyName);

                    WriteJsonValue(jsObj, isRoot, filterProperties, propertyName, jsPropertyValue);
                }
            }
        }

        private IEnumerable<KeyValuePair<string, InternalHandle>> GetBoundObjectProperties(InternalHandle jsObj)
        {
            object obj = jsObj.BoundObject;
            if (obj == null)
                yield break;

            if (obj is IDictionary dictionary)
            {
                foreach (DictionaryEntry entry in dictionary)
                {
                    yield return new KeyValuePair<string, InternalHandle>(entry.Key.ToString(), _engine.FromObject(entry.Value));
                }
                yield break;
            }

            var type = obj.GetType();
            var binder = (ObjectBinder)jsObj.Object;
            // look for properties
            foreach (var property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (property.CanRead == false)
                    continue;
    
                string name = property.Name;
                yield return new KeyValuePair<string, InternalHandle>(property.Name, binder.NamedPropertyGetter(ref name));
            }

            // look for fields
            foreach (var field in type.GetFields(BindingFlags.Instance | BindingFlags.Public))
            {
                string name = field.Name;
                yield return new KeyValuePair<string, InternalHandle>(field.Name, binder.NamedPropertyGetter(ref name));
            }
        }

        private unsafe void WriteBlittableInstance(InternalHandle jsObj, bool isRoot, bool filterProperties)
        {
            object boundObject = jsObj.BoundObject;
            var obj = boundObject as BlittableObjectInstanceV8;

            HashSet<string> modifiedProperties = null;
            if (obj.DocumentId != null &&
                _usageMode == BlittableJsonDocumentBuilder.UsageMode.None)
            {
                using (InternalHandle metadata = obj.GetOrCreateV8(Constants.Documents.Metadata.Key))
                {
                    if (metadata.IsBinder && metadata.BoundObject is BlittableObjectInstanceV8 boi) { // to avoid V8 participation
                        using (var jsDocId = jsObj.Engine.CreateValue(obj.DocumentId))
                            boi.SetOwnProperty(Constants.Documents.Metadata.Id, jsDocId, toReturnCopy: false);
                    }
                    else
                        metadata.SetProperty(Constants.Documents.Metadata.Id, obj.DocumentId);
                }
            }
            if (obj.Blittable != null)
            {
                var propertiesByInsertionOrder = obj.Blittable.GetPropertiesByInsertionOrder();
                for (int i = 0; i < propertiesByInsertionOrder.Size; i++)
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();
                    var propIndex = propertiesByInsertionOrder.Properties[i];
                    obj.Blittable.GetPropertyByIndex(propIndex, ref prop);

                    IBlittableObjectProperty modifiedValue = default;
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
                        WriteJsonValue(jsObj, isRoot, filterProperties, prop.Name, modifiedValue.ValueHandle.V8.Item);
                    }
                    else
                    {
                        _writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                    }
                }
            }

            if (obj.OwnValues == null)
                return;

            foreach (var modificationKvp in obj.OwnValues)
            {
                //We already iterated through those properties while iterating the original properties set.
                if (modifiedProperties != null && modifiedProperties.Contains(modificationKvp.Key))
                    continue;

                var propertyName = modificationKvp.Key;
                if (ShouldFilterProperty(filterProperties, propertyName))
                    continue;

                if (modificationKvp.Value.Changed == false)
                    continue;

                _writer.WritePropertyName(propertyName);
                var blittableObjectProperty = modificationKvp.Value;
                WriteJsonValue(jsObj, isRoot, filterProperties, propertyName, blittableObjectProperty.Value);
            }
        }

        public static BlittableJsonReaderObject Translate(JsonOperationContext context, V8Engine engine, InternalHandle objectInstance, 
            IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, 
            bool isRoot = true)
        {
            if (objectInstance.IsUndefined || objectInstance.IsNull)
                return null;

            object boundObject = objectInstance.BoundObject;
            if (boundObject != null && boundObject is BlittableObjectInstanceV8 boi && boi.Changed == false && isRoot)
                return boi.Blittable.Clone(context);

            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                writer.Reset(usageMode);
                writer.StartWriteObjectDocument();

                var blittableBridge = new JsBlittableBridgeV8(writer, usageMode, (V8EngineEx)engine);
                blittableBridge.WriteInstance(objectInstance, modifier, isRoot, filterProperties: false);

                writer.FinalizeDocument();

                return writer.CreateReader();
            }
        }
    }
}
