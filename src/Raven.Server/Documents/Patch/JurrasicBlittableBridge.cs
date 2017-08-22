using System;
using System.Collections.Generic;
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
        private readonly BlittableJsonDocumentBuilder.UsageMode _usageMode;

        [ThreadStatic]
        private static HashSet<object> _recursive;

        public JurrasicBlittableBridge(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, BlittableJsonDocumentBuilder.UsageMode usageMode)
        {
            _writer = writer;
            _usageMode = usageMode;
        }

        public void WriteInstance(ObjectInstance jsObject)
        {
            _writer.StartWriteObject();
            WriteRawObjectProperties(jsObject);
            _writer.WriteObjectEnd();
        }

        private void WriteRawObjectProperties(ObjectInstance jsObject)
        {
            var blittableObjectInstance = jsObject as BlittableObjectInstance;

            if (blittableObjectInstance != null)
                WriteBlittableInstance(blittableObjectInstance);
            else
                WriteJurrasicInstance(jsObject);
        }

        private void WriteJsonValue(object value)
        {
            if (value is NullObjectInstance)
            {
                _writer.WriteValueNull();
                return;
            }
            WriteValue(value);
        }

        private void WriteValue(object v)
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
            {
                if (Math.Abs(Math.Round(d, 0) - d) < double.Epsilon)
                {
                    _writer.WriteValue((long)d);

                }
                else
                {
                    _writer.WriteValue(d);
                }
            }
            else if (v == Null.Value || v == Undefined.Value)
                _writer.WriteValueNull();
            else if (v is ArrayInstance jsArray)
            {
                _writer.StartWriteArray();
                foreach (var property in jsArray.Properties)
                {
                    if((property.Attributes & PropertyAttributes.IsLengthProperty)==PropertyAttributes.IsLengthProperty)
                        continue;
                    WriteValue(property.Value);
                }
                _writer.WriteArrayEnd();
            }
            else if (v is RegExpInstance)
            {
                _writer.WriteValueNull();
            }
            else if (v is ObjectInstance obj)
            {
                if (_recursive == null)
                    _recursive = new HashSet<object>();
                try
                {
                    if(_recursive.Add(obj))
                        WriteInstance(obj);
                    else
                        _writer.WriteValueNull();
                }
                finally
                {
                    _recursive.Remove(obj);
                }
            }
            else if (v is ConcatenatedString cs)
            {
                _writer.WriteValue(cs.ToString());
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

        private void WriteJurrasicInstance(ObjectInstance jsObject)
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
                WriteJsonValue(value);
            }
        }

        private void WriteBlittableInstance(BlittableObjectInstance obj)
        {
            if (obj.DocumentId != null && 
                _usageMode == BlittableJsonDocumentBuilder.UsageMode.None)
            {
                var metadata = ((ObjectInstance)obj[Constants.Documents.Metadata.Key]);
                metadata[Constants.Documents.Metadata.Id] = obj.DocumentId;
            }
            var properties = obj.Properties.ToDictionary(x => x.Key.ToString(), x => x.Value);
            foreach (var propertyIndex in obj.Blittable.GetPropertiesByInsertionOrder())
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                obj.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                if (obj.Deletes?.Contains(prop.Name) == true)
                    continue;

                _writer.WritePropertyName(prop.Name);

                if (properties.Remove(prop.Name, out var modifiedValue))
                    WriteJsonValue(modifiedValue);
                else
                    _writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
            }

            foreach (var modificationKvp in properties)
            {
                _writer.WritePropertyName(modificationKvp.Key);
                WriteJsonValue(modificationKvp.Value);
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
            if (objectInstance == null)
                return null;

            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                writer.Reset(usageMode);
                writer.StartWriteObjectDocument();

                var jurrasicBlittableBridge = new JurrasicBlittableBridge(writer, usageMode);
                jurrasicBlittableBridge.WriteInstance(objectInstance);

                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }
    }
}
