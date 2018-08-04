using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json
{
    internal class BlittableJsonWriter : JsonWriter
    {
        private readonly ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> _manualBlittableJsonDocumentBuilder;
        private bool _first;
        private readonly DocumentInfo _documentInfo;

        public BlittableJsonWriter(JsonOperationContext context, DocumentInfo documentInfo = null,
            BlittableJsonDocumentBuilder.UsageMode? mode = null, BlittableWriter<UnmanagedWriteBuffer> writer = null)
        {
            _manualBlittableJsonDocumentBuilder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context, mode ?? BlittableJsonDocumentBuilder.UsageMode.None, writer);
            _manualBlittableJsonDocumentBuilder.Reset(mode ?? BlittableJsonDocumentBuilder.UsageMode.None);
            _manualBlittableJsonDocumentBuilder.StartWriteObjectDocument();
            _documentInfo = documentInfo;
            _first = true;
        }

        public override void WriteStartObject()
        {
            _manualBlittableJsonDocumentBuilder.StartWriteObject();
            if (!_first)
                return;
            _first = false;
            WriteMetadata();
        }

        private void WriteMetadata()
        {
            if (_documentInfo == null)
                return;
            if (_documentInfo.Metadata?.Modifications != null && (_documentInfo.Metadata.Modifications.Properties.Count > 0))
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Key);
                _manualBlittableJsonDocumentBuilder.StartWriteObject();

                foreach (var prop in _documentInfo.Metadata.Modifications.Properties)
                {
                    if (prop.Name.Length > 0 && prop.Name[0] == '@')
                    {
                        if (prop.Name != Constants.Documents.Metadata.Collection && 
                            prop.Name != Constants.Documents.Metadata.Expires && 
                            prop.Name != Constants.Documents.Metadata.Edges)
                            continue; // system property, ignoring it
                    }

                    _manualBlittableJsonDocumentBuilder.WritePropertyName(prop.Item1);
                    WritePropertyValue(prop.Name, prop.Value);
                }

                if (_documentInfo.Collection != null)
                {
                    _manualBlittableJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Collection);
                    _manualBlittableJsonDocumentBuilder.WriteValue(_documentInfo.Collection);
                }

                _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
                _documentInfo.Metadata.Modifications = null;
            }
            else if (_documentInfo.Metadata != null)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Key);
                _manualBlittableJsonDocumentBuilder.StartWriteObject();
                var ids = _documentInfo.Metadata.GetPropertiesByInsertionOrder();

                foreach (var id in ids)
                {
                    var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                    _documentInfo.Metadata.GetPropertyByIndex(id, ref propertyDetails);
                    _manualBlittableJsonDocumentBuilder.WritePropertyName(propertyDetails.Name);

                    WritePropertyValue(propertyDetails);
                }
                _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
            }
            else if (_documentInfo.MetadataInstance != null)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Key);
                _manualBlittableJsonDocumentBuilder.StartWriteObject();

                foreach (var kvp in _documentInfo.MetadataInstance)
                {
                    _manualBlittableJsonDocumentBuilder.WritePropertyName(kvp.Key);

                    WritePropertyValue(kvp.Key, kvp.Value);
                }
                _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
            }
            else if (_documentInfo.Collection != null)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Key);
                _manualBlittableJsonDocumentBuilder.StartWriteObject();

                _manualBlittableJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Collection);
                _manualBlittableJsonDocumentBuilder.WriteValue(_documentInfo.Collection);

                if (_documentInfo.Id != null)
                {
                    _manualBlittableJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Id);
                    _manualBlittableJsonDocumentBuilder.WriteValue(_documentInfo.Id);
                }

                _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
            }
        }

        private void WritePropertyValue(BlittableJsonReaderObject.PropertyDetails prop)
        {
            switch (prop.Token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.StartObject:
                    if (prop.Value is BlittableJsonReaderObject obj)
                        WriteObject(obj);
                    break;
                case BlittableJsonToken.StartArray:
                    _manualBlittableJsonDocumentBuilder.StartWriteArray();
                    if (prop.Value is BlittableJsonReaderArray array)
                    {
                        foreach (var entry in array)
                        {
                            if (entry is BlittableJsonReaderObject entryObj)
                                WriteObject(entryObj);
                            else
                                WritePropertyValue(prop.Name, entry);
                        }
                    }
                    _manualBlittableJsonDocumentBuilder.WriteArrayEnd();
                    break;
                case BlittableJsonToken.Integer:
                    _manualBlittableJsonDocumentBuilder.WriteValue((long)prop.Value);
                    break;
                case BlittableJsonToken.LazyNumber:
                    if (prop.Value is LazyNumberValue ldv)
                    {
                        _manualBlittableJsonDocumentBuilder.WriteValue(ldv);
                    }
                    else if (prop.Value is double d)
                    {
                        _manualBlittableJsonDocumentBuilder.WriteValue(d);
                    }
                    else
                    {
                        _manualBlittableJsonDocumentBuilder.WriteValue((float)prop.Value);
                    }
                    break;
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                    _manualBlittableJsonDocumentBuilder.WriteValue(prop.Value.ToString());
                    break;
                case BlittableJsonToken.Boolean:
                    _manualBlittableJsonDocumentBuilder.WriteValue((bool)prop.Value);
                    break;
                case BlittableJsonToken.Null:
                    _manualBlittableJsonDocumentBuilder.WriteValueNull();
                    break;
                default:
                    throw new NotSupportedException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteObject(BlittableJsonReaderObject obj)
        {
            var propDetails = new BlittableJsonReaderObject.PropertyDetails();

            _manualBlittableJsonDocumentBuilder.StartWriteObject();
            var propsIndexes = obj.GetPropertiesByInsertionOrder();
            foreach (var index in propsIndexes)
            {
                obj.GetPropertyByIndex(index, ref propDetails);
                _manualBlittableJsonDocumentBuilder.WritePropertyName(propDetails.Name);
                WritePropertyValue(propDetails);
            }

            _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
        }

        private void WritePropertyValue(string propName, object value)
        {
            switch (value)
            {
                case null:
                    _manualBlittableJsonDocumentBuilder.WriteValueNull();
                    break;
                case string strValue:
                    _manualBlittableJsonDocumentBuilder.WriteValue(strValue);
                    break;
                case LazyStringValue lazyStringValue:
                    _manualBlittableJsonDocumentBuilder.WriteValue(lazyStringValue);
                    break;
                case long l:
                    _manualBlittableJsonDocumentBuilder.WriteValue(l);
                    break;
                case double d:
                    _manualBlittableJsonDocumentBuilder.WriteValue(d);
                    break;
                case decimal decVal:
                    _manualBlittableJsonDocumentBuilder.WriteValue(decVal);
                    break;
                case float f:
                    _manualBlittableJsonDocumentBuilder.WriteValue(f);
                    break;
                case bool b:
                    _manualBlittableJsonDocumentBuilder.WriteValue(b);
                    break;
                case LazyNumberValue lazyNumber:
                    _manualBlittableJsonDocumentBuilder.WriteValue(lazyNumber);
                    break;
                case DateTime dt:
                    _manualBlittableJsonDocumentBuilder.WriteValue(dt.GetDefaultRavenFormat(isUtc: dt.Kind == DateTimeKind.Utc));
                    break;
                case DateTimeOffset dto:
                    _manualBlittableJsonDocumentBuilder.WriteValue(dto.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
                    break;
                case IDictionary<string, string> dics:
                    WriteDictionary(dics);
                    break;
                case IDictionary<string, object> dico:
                    WriteDictionary(dico);
                    break;
                case DynamicJsonValue val:
                    foreach (var prop in val.Properties)
                    {
                        WritePropertyValue(prop.Name,prop.Value);
                    }
                    break;
                case IEnumerable enumerable:
                    _manualBlittableJsonDocumentBuilder.StartWriteArray();
                    foreach (var entry in enumerable)
                    {
                        WritePropertyValue(propName, entry);
                    }
                    _manualBlittableJsonDocumentBuilder.WriteArrayEnd();
                    break;

                default:
                    throw new NotSupportedException($"The value type {value.GetType().FullName} of key {propName} is not supported in the metadata");
            }
        }

        private void WriteDictionary<T>(IDictionary<string, T> dic)
        {
            _manualBlittableJsonDocumentBuilder.StartWriteObject();
            foreach (var item in dic)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(item.Key);
                WritePropertyValue(item.Key, item.Value);
            }
            _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
        }

        public override void WriteEndObject()
        {
            _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
        }

        public void FinalizeDocument()
        {
            _manualBlittableJsonDocumentBuilder.FinalizeDocument();
        }

        public override void WriteStartArray()
        {
            _manualBlittableJsonDocumentBuilder.StartWriteArray();
        }

        public override void WriteEndArray()
        {
            _manualBlittableJsonDocumentBuilder.WriteArrayEnd();
        }

        public override void WritePropertyName(string name)
        {
            _manualBlittableJsonDocumentBuilder.WritePropertyName(name);
        }

        public override void WritePropertyName(string name, bool escape)
        {
            _manualBlittableJsonDocumentBuilder.WritePropertyName(name);
        }

        public override void WriteNull()
        {
            _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(string value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(int value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(long value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(float value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(double value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(bool value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(short value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(byte value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(decimal value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(DateTime value)
        {
            var s = value.GetDefaultRavenFormat(isUtc: value.Kind == DateTimeKind.Utc);
            _manualBlittableJsonDocumentBuilder.WriteValue(s);
        }

        public override void WriteValue(DateTimeOffset value)
        {
            var s = value.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
            _manualBlittableJsonDocumentBuilder.WriteValue(s);
        }

        public override void WriteValue(int? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(long? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(float? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(double? value)
        {
            if (value != null)
            {

                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            }
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(bool? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(short? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(byte? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(decimal? value)
        {
            if (value != null)
            {
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            }
            else
            {
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
            }

        }

        public override void WriteValue(DateTime? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.GetDefaultRavenFormat(isUtc: value.Value.Kind == DateTimeKind.Utc));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(DateTimeOffset? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        protected override void Dispose(bool disposing)
        {
            _manualBlittableJsonDocumentBuilder.Dispose();
        }

        public BlittableJsonReaderObject CreateReader()
        {
            return _manualBlittableJsonDocumentBuilder.CreateReader();
        }

        public override void WriteValue(Guid value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public override void WriteValue(Guid? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString());
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(char value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public override void WriteValue(char? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString());
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(sbyte value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(sbyte? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(uint value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(uint? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(ushort value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(ushort? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(ulong value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(ulong? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(TimeSpan value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString("c"));
        }

        public override void WriteValue(TimeSpan? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString("c"));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(byte[] value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(Convert.ToBase64String(value));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(Uri value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(object value)
        {
            switch (value)
            {
                case BlittableJsonReaderObject readerObject:
                    if (false == readerObject.HasParent)
                    {
                        _manualBlittableJsonDocumentBuilder.WriteEmbeddedBlittableDocument(readerObject);
                    }
                    else
                    {
                        using (var clonedBlittable = readerObject.CloneOnTheSameContext())
                        {
                            _manualBlittableJsonDocumentBuilder.WriteEmbeddedBlittableDocument(clonedBlittable);
                        }
                    }
                    return;
                case LazyStringValue lazyStringValue:
                    _manualBlittableJsonDocumentBuilder.WriteValue(lazyStringValue);
                    return;
            }
            base.WriteValue(value);
        }

        public override void WriteComment(string text)
        {
            throw new NotSupportedException();
        }

        public override void WriteWhitespace(string ws)
        {
            throw new NotSupportedException();
        }

        public override string ToString()
        {
            throw new NotSupportedException();
        }

        public override int GetHashCode()
        {
            throw new NotSupportedException();
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override void Close()
        {
            throw new NotSupportedException();
        }

        public override void WriteStartConstructor(string name)
        {
            throw new NotSupportedException();
        }

        public override void WriteEndConstructor()
        {
            throw new NotSupportedException();
        }

        public override void WriteEnd()
        {
            throw new NotSupportedException();
        }

        protected override void WriteEnd(JsonToken token)
        {
            throw new NotSupportedException();
        }

        protected override void WriteIndent()
        {
            throw new NotSupportedException();
        }

        protected override void WriteValueDelimiter()
        {
            throw new NotSupportedException();
        }

        protected override void WriteIndentSpace()
        {
            throw new NotSupportedException();
        }

        public override void WriteUndefined()
        {
            throw new NotSupportedException();
        }

        public override void WriteRaw(string json)
        {
            throw new NotSupportedException();
        }

        public override void WriteRawValue(string json)
        {
            throw new NotSupportedException();
        }

        public override bool Equals(object obj)
        {
            throw new NotSupportedException();
        }
    }
}
