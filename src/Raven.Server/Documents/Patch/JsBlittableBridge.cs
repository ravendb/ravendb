using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Native.RegExp;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Descriptors.Specialized;
using Jint.Runtime.Interop;
using Raven.Client;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public struct JsBlittableBridge
    {
        private readonly ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> _writer;
        private readonly BlittableJsonDocumentBuilder.UsageMode _usageMode;
        private readonly Engine _scriptEngine;

        [ThreadStatic]
        private static HashSet<object> _recursive;

        public JsBlittableBridge(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, BlittableJsonDocumentBuilder.UsageMode usageMode, Engine scriptEngine)
        {
            _writer = writer;
            _usageMode = usageMode;
            _scriptEngine = scriptEngine;
        }

        public static void CleanCache()
        {            
            _recursive = null;
        }

        public void WriteInstance(ObjectInstance jsObject, IResultModifier modifier = null)
        {
            _writer.StartWriteObject();

            modifier?.Modify(jsObject);
            WriteRawObjectProperties(jsObject);

            _writer.WriteObjectEnd();
        }

        private void WriteRawObjectProperties(ObjectInstance jsObject)
        {
            var blittableObjectInstance = jsObject as BlittableObjectInstance;

            if (blittableObjectInstance != null)
                WriteBlittableInstance(blittableObjectInstance);
            else
                WriteJsInstance(jsObject);
        }

        private void WriteJsonValue(object parent, string propName, object value)
        {
            if (value is JsValue js)
            {
                if (js.IsBoolean())
                    _writer.WriteValue(js.AsBoolean());
                else if (js.IsUndefined() || js.IsNull())
                    _writer.WriteValueNull();
                else if (js.IsString())
                    _writer.WriteValue(js.AsString());
                else if (js.IsDate())
                    _writer.WriteValue(js.AsDate().ToDateTime().ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
                else if (js.IsNumber())
                    WriteNumber(parent, propName, js.AsNumber());
                else if (js.IsArray())
                    WriteArray(js.AsArray());
                else if (js.IsObject())
                {
                    WriteNestedObject(js.AsObject());
                }
                else
                {
                    throw new InvalidOperationException("Unknonw type: " + js.Type);
                }
                return;
            }
            WriteValue(parent, propName, value);
        }

        private void WriteArray(ArrayInstance arrayInstance)
        {
            _writer.StartWriteArray();
            foreach (var property in arrayInstance.GetOwnProperties())
            {
                if (property.Key == "length")
                    continue;
                WriteJsonValue(arrayInstance, property.Key, SafelyGetJsValue(property.Value));
            }
            _writer.WriteArrayEnd();
        }

        private void WriteValue(object parent, string propName, object v)
        {
            if (v is bool b)
                _writer.WriteValue(b);
            else if (v is string s)
                _writer.WriteValue(s);
            else if (v is byte by)
                _writer.WriteValue(by);
            else if (v is int i)
                WriteNumber(parent, propName, i);
            else if (v is uint ui)
                _writer.WriteValue(ui);
            else if (v is long l)
                _writer.WriteValue(l);
            else if (v is double d)
            {
                WriteNumber(parent, propName, d);
            }
            else if (v == null || ReferenceEquals(v, Null.Instance) || ReferenceEquals(v, Undefined.Instance))
                _writer.WriteValueNull();
            else if (v is ArrayInstance jsArray)
            {
                _writer.StartWriteArray();
                foreach (var property in jsArray.GetOwnProperties())
                {
                    WriteValue(jsArray, property.Key as string, property.Value);
                }
                _writer.WriteArrayEnd();
            }
            else if (v is RegExpInstance)
            {
                _writer.WriteValueNull();
            }
            else if (v is ObjectInstance obj)
            {
                WriteNestedObject(obj);
            }
            else if (v is LazyStringValue lsv)
            {
                _writer.WriteValue(lsv);
            }
            else if (v is LazyCompressedStringValue lcsv)
            {
                _writer.WriteValue(lcsv);
            }
            else if (v is LazyNumberValue lnv)
            {
                _writer.WriteValue(lnv);
            }
            else
            {
                throw new NotSupportedException(v.GetType().ToString());
            }
        }

        private void WriteNestedObject(ObjectInstance obj)
        {
            if (_recursive == null)
                _recursive = new HashSet<object>();

            if (obj is ObjectWrapper objectWrapper)
            {
                var target = objectWrapper.Target;

                if (target is IDictionary dictionary)
                {
                    WriteValueInternal(target, obj);
                }
                else if (target is IEnumerable enumerable)
                {
                    var jsArray = (ArrayInstance)_scriptEngine.Array.Construct(Arguments.Empty);
                    foreach (var item in enumerable)
                    {
                        var jsItem = JsValue.FromObject(_scriptEngine, item);
                        _scriptEngine.Array.PrototypeObject.Push(jsArray, Arguments.From(jsItem));
                    }
                    WriteArray(jsArray);
                }
                else
                    WriteObjectType(target);
            }
            else if (obj is FunctionInstance)
                _writer.WriteValueNull();
            else
                WriteValueInternal(obj, obj);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteObjectType(object target)
        {
            _writer.WriteValue('[' + target.GetType().Name + ']');
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteValueInternal(object target, ObjectInstance obj)
        {
            try
            {
                if (_recursive.Add(target))
                    WriteInstance(obj);
                else
                    _writer.WriteValueNull();
            }
            finally
            {
                _recursive.Remove(target);
            }
        }

        private void WriteNumber(object parent, string propName, double d)
        {
            var writer = _writer;
            var boi = parent as BlittableObjectInstance;
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
                if (type == BlittableJsonToken.Integer)
                {
                    writer.WriteValue((long)d);
                    return true;
                }
                if (type == BlittableJsonToken.LazyNumber)
                {
                    writer.WriteValue(d);
                    return true;
                }
                return false;
            }

            void GuessNumberType()
            {
                if (Math.Abs(Math.Round(d, 0) - d) < double.Epsilon)
                {
                    writer.WriteValue((long)d);
                }
                else
                {
                    writer.WriteValue(d);
                }
            }
        }

        private void WriteJsInstance(ObjectInstance jsObject)
        {
            var properties = jsObject is ObjectWrapper objectWrapper
                ? GetObjectProperties(objectWrapper)
                : jsObject.GetOwnProperties();

            foreach (var property in properties)
            {
                var propertyName = property.Key;
                if (ShouldFilterProperty(propertyName))
                    continue;

                var value = property.Value;
                if (value == null)
                    continue;

                _writer.WritePropertyName(propertyName);
                WriteJsonValue(jsObject, propertyName, SafelyGetJsValue(value));
            }
        }

        private IEnumerable<KeyValuePair<string, PropertyDescriptor>> GetObjectProperties(ObjectWrapper objectWrapper)
        {
            var target = objectWrapper.Target;
            if (target is IDictionary dictionary)
            {
                foreach (DictionaryEntry entry in dictionary)
                {
                    var jsValue = JsValue.FromObject(_scriptEngine, entry.Value);
                    var descriptor = new PropertyDescriptor(jsValue, false, false, false);
                    yield return new KeyValuePair<string, PropertyDescriptor>(entry.Key.ToString(), descriptor);
                }
                yield break;
            }

            var type = target.GetType();
            if (target is Task task &&
                task.IsCompleted == false)
            {
                foreach (var property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    if (property.CanRead == false)
                        continue;

                    if (property.Name == nameof(Task<int>.Result))
                    {
                        var taskResultDescriptor = JintPreventResolvingTasksReferenceResolver.GetRunningTaskResult(task);
                        yield return new KeyValuePair<string, PropertyDescriptor>(property.Name, taskResultDescriptor);
                        continue;
                    }

                    var descriptor = new PropertyInfoDescriptor(_scriptEngine, property, target);
                    yield return new KeyValuePair<string, PropertyDescriptor>(property.Name, descriptor);
                }
                yield break;
            }

            // look for properties
            foreach (var property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (property.CanRead == false)
                    continue;

                var descriptor = new PropertyInfoDescriptor(_scriptEngine, property, target);
                yield return new KeyValuePair<string, PropertyDescriptor>(property.Name, descriptor);
            }

            // look for fields
            foreach (var field in type.GetFields(BindingFlags.Instance | BindingFlags.Public))
            {
                var descriptor = new FieldInfoDescriptor(_scriptEngine, field, target);
                yield return new KeyValuePair<string, PropertyDescriptor>(field.Name, descriptor);
            }
        }

        private JsValue SafelyGetJsValue(PropertyDescriptor property)
        {
            try
            {
                return property.Value;
            }
            catch (Exception e)
            {
                return new JsValue(e.ToString());
            }
        }

        private void WriteBlittableInstance(BlittableObjectInstance obj)
        {
            if (obj.DocumentId != null &&
                _usageMode == BlittableJsonDocumentBuilder.UsageMode.None)
            {
                var metadata = obj.GetOrCreate(Constants.Documents.Metadata.Key);
                metadata.Put(Constants.Documents.Metadata.Id, obj.DocumentId, false);
            }
            if (obj.Blittable != null)
            {
                foreach (var propertyIndex in obj.Blittable.GetPropertiesByInsertionOrder())
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();

                    obj.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                    var existInObject = obj.OwnValues.Remove(prop.Name, out var modifiedValue);

                    if (existInObject == false && obj.Deletes?.Contains(prop.Name) == true)
                        continue;

                    _writer.WritePropertyName(prop.Name);

                    if (existInObject && modifiedValue.Changed)
                    {
                        WriteJsonValue(obj, prop.Name, modifiedValue.Value);
                    }
                    else
                    {
                        _writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                    }
                }
            }

            foreach (var modificationKvp in obj.OwnValues)
            {
                _writer.WritePropertyName(modificationKvp.Key);
                var blittableObjectProperty = modificationKvp.Value;
                WriteJsonValue(obj, modificationKvp.Key, blittableObjectProperty.Value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldFilterProperty(string property)
        {
            return property == Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName ||
                   property == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
                   property == Constants.Documents.Metadata.Id ||
                   property == Constants.Documents.Metadata.LastModified ||
                   property == Constants.Documents.Metadata.IndexScore ||
                   property == Constants.Documents.Metadata.ChangeVector ||
                   property == Constants.Documents.Metadata.Flags;
        }

        public static BlittableJsonReaderObject Translate(JsonOperationContext context, Engine scriptEngine, ObjectInstance objectInstance, IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (objectInstance == null)
                return null;

            if (objectInstance is BlittableObjectInstance boi && boi.Changed == false)
                return boi.Blittable.Clone(context);

            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                writer.Reset(usageMode);
                writer.StartWriteObjectDocument();

                var blittableBridge = new JsBlittableBridge(writer, usageMode, scriptEngine);
                blittableBridge.WriteInstance(objectInstance, modifier);

                writer.FinalizeDocument();

                return writer.CreateReader();
            }
        }

        public interface IResultModifier
        {
            void Modify(ObjectInstance json);
        }
    }
}
