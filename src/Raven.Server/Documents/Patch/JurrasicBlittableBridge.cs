using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Jurassic;
using Jurassic.Library;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public struct JurrasicBlittableBridge
    {
        private readonly ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> _writer;

        public JurrasicBlittableBridge(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer)
        {
            _writer = writer;
        }


        public void WriteInstance(
            ObjectInstance jsObject,
            bool recursiveCall = false)
        {
            _writer.StartWriteObject();
            WriteRawObjectProperties(jsObject, recursiveCall);
            _writer.WriteObjectEnd();
        }

        private void WriteRawObjectProperties(
            ObjectInstance jsObject,
            bool recursiveCall = false)
        {
            var blittableObjectInstance = jsObject as BlittableObjectInstance;

            if (blittableObjectInstance != null)
                WriteBlittableInstance(blittableObjectInstance, recursiveCall);
            else
                WriteJurrasicInstance(jsObject, recursiveCall);
        }

        private void WriteJsonValue(object current, object value, bool recursiveCall)
        {
            var recursive = current == value;
            if (recursiveCall && recursive || value is NullObjectInstance)
            {
                _writer.WriteValueNull();
                return;
            }
            WriteValue(value, recursive);
        }

        private void WriteValue(object v, bool recursiveCall)
        {
            if (v is bool b)
                _writer.WriteValue(b);
            else if (v is string s)
                _writer.WriteValue(s);
            else if (v is byte by)
                _writer.WriteValue(by);
            else if (v is int i)
                _writer.WriteValue(i);
            else if (v is uint ui)
                _writer.WriteValue(ui);
            else if (v is long l)
                _writer.WriteValue(l);
            else if (v is double d)
                _writer.WriteValue(d);
            else if (v == Null.Value || v == Undefined.Value)
                _writer.WriteValueNull();
            else if (v is ArrayInstance jsArray)
            {
                _writer.StartWriteArray();
                foreach (var property in jsArray.Properties)
                {
                    var jsInstance = property.Value;
                    if (jsInstance == null)
                        continue;

                    WriteValue(jsInstance, recursiveCall);
                }
                _writer.WriteArrayEnd();
            }
            else if (v is RegExpInstance)
            {
                _writer.WriteValueNull();
            }
            else if (v is ObjectInstance obj)
            {
                WriteInstance(obj, recursiveCall);
            }
            else
            {
                throw new NotSupportedException(v.GetType().ToString());
            }
        }

        private void WriteJurrasicInstance(ObjectInstance jsObject, bool recursiveCall)
        {
            foreach (var property in jsObject.Properties)
            {
                var propertyName = property.Key.ToString();
                if (ShouldFilterProperty(propertyName))
                    continue;

                var value = property.Value;
                if (value == null)
                    continue;

                if (value is RegExpInstance)
                    continue;

                _writer.WritePropertyName(propertyName);
                WriteJsonValue(jsObject, value, recursiveCall);
            }
        }

        private void WriteBlittableInstance(BlittableObjectInstance blittableObjectInstance, bool recursiveCall)
        {
            var properties = blittableObjectInstance.Properties.ToDictionary(x => x.Key.ToString(), x => x.Value);
            foreach (var propertyIndex in blittableObjectInstance.Blittable.GetPropertiesByInsertionOrder())
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                if (blittableObjectInstance.Deletes?.Contains(prop.Name) == true)
                    continue;

                _writer.WritePropertyName(prop.Name);

                if (properties.Remove(prop.Name, out var modifiedValue))
                    WriteJsonValue(prop.Name, modifiedValue, recursiveCall);
                else
                    _writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
            }

            foreach (var modificationKvp in properties)
            {
                _writer.WritePropertyName(modificationKvp.Key);
                WriteJsonValue(modificationKvp.Key, modificationKvp.Value, recursiveCall);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldFilterProperty(string property)
        {
            return property == Constants.Documents.Indexing.Fields.ReduceKeyFieldName ||
                   property == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
                   property == Constants.Documents.Metadata.Id ||
                   property == Constants.Documents.Metadata.LastModified ||
                   property == Constants.Documents.Metadata.IndexScore ||
                   property == Constants.Documents.Metadata.ChangeVector ||
                   property == Constants.Documents.Metadata.Flags;
        }

        public static BlittableJsonReaderObject Translate(JsonOperationContext context, ObjectInstance objectInstance, 
            BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                writer.Reset(usageMode);
                writer.StartWriteObjectDocument();

                new JurrasicBlittableBridge(writer).WriteInstance(objectInstance);

                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }
    }
}
