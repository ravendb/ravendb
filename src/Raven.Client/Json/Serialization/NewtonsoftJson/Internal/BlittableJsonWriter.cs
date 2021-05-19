using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
{
    internal class BlittableJsonWriter : JsonWriter, IJsonWriter
    {
        private readonly LazyStringValue _metadataKey;
        private readonly LazyStringValue _metadataCollection;
        private readonly LazyStringValue _metadataId;


        private readonly ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> _manualBlittableJsonDocumentBuilder;
        private bool _first;
        private readonly DocumentInfo _documentInfo;

        public BlittableJsonWriter(JsonOperationContext context, DocumentInfo documentInfo = null,
            BlittableJsonDocumentBuilder.UsageMode? mode = null, BlittableWriter<UnmanagedWriteBuffer> writer = null,
            LazyStringValue idField = null, LazyStringValue keyField = null, LazyStringValue collectionField = null)
        {
            _manualBlittableJsonDocumentBuilder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context, mode ?? BlittableJsonDocumentBuilder.UsageMode.None, writer);
            _manualBlittableJsonDocumentBuilder.Reset(mode ?? BlittableJsonDocumentBuilder.UsageMode.None);
            _manualBlittableJsonDocumentBuilder.StartWriteObjectDocument();
            _documentInfo = documentInfo;
            _first = true;

            _metadataId = idField ?? context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Id);
            _metadataKey = keyField ?? context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Key);
            _metadataCollection = collectionField ?? context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Collection);
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
            if (_documentInfo.Metadata?.Modifications != null && _documentInfo.Metadata.Modifications.Properties.Count > 0)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(_metadataKey);
                _manualBlittableJsonDocumentBuilder.StartWriteObject();

                foreach (var prop in _documentInfo.Metadata.Modifications.Properties)
                {
                    if (prop.Name.Length > 0 && prop.Name[0] == '@')
                    {
                        if (prop.Name != Constants.Documents.Metadata.Collection &&
                            prop.Name != Constants.Documents.Metadata.Expires &&
                            prop.Name != Constants.Documents.Metadata.Refresh &&
                            prop.Name != Constants.Documents.Metadata.ClusterTransactionIndex &&
                            prop.Name != Constants.Documents.Metadata.Edges)
                            continue; // system property, ignoring it
                    }

                    _manualBlittableJsonDocumentBuilder.WritePropertyName(prop.Item1);
                    WritePropertyValue(prop.Name, prop.Value);
                }

                if (_documentInfo.Collection != null)
                {
                    _manualBlittableJsonDocumentBuilder.WritePropertyName(_metadataCollection);
                    _manualBlittableJsonDocumentBuilder.WriteValue(_documentInfo.Collection);
                }

                _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
                _documentInfo.Metadata.Modifications = null;
            }
            else if (_documentInfo.Metadata != null)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(_metadataKey);
                _manualBlittableJsonDocumentBuilder.StartWriteObject();

                for (int i = 0; i < _documentInfo.Metadata.Count; i++)
                {
                    var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                    _documentInfo.Metadata.GetPropertyByIndex(i, ref propertyDetails);
                    _manualBlittableJsonDocumentBuilder.WritePropertyName(propertyDetails.Name);

                    WritePropertyValue(propertyDetails);
                }
                _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
            }
            else if (_documentInfo.MetadataInstance != null)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(_metadataKey);
                WriteMetadata(_documentInfo.MetadataInstance);
            }
            else if (_documentInfo.Collection != null)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(_metadataKey);
                _manualBlittableJsonDocumentBuilder.StartWriteObject();

                _manualBlittableJsonDocumentBuilder.WritePropertyName(_metadataCollection);
                _manualBlittableJsonDocumentBuilder.WriteValue(_documentInfo.Collection);

                if (_documentInfo.Id != null)
                {
                    _manualBlittableJsonDocumentBuilder.WritePropertyName(_metadataId);
                    _manualBlittableJsonDocumentBuilder.WriteValue(_documentInfo.Id);
                }

                _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
            }
        }

        public void WriteMetadata(IMetadataDictionary metadata)
        {
            _manualBlittableJsonDocumentBuilder.StartWriteObject();

            foreach (var kvp in metadata)
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(kvp.Key);

                WritePropertyValue(kvp.Key, kvp.Value);
            }

            _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
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
                    _manualBlittableJsonDocumentBuilder.WriteValue((LazyStringValue)prop.Value);
                    break;
                case BlittableJsonToken.CompressedString:
                    _manualBlittableJsonDocumentBuilder.WriteValue((LazyCompressedStringValue)prop.Value);
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
            for (int i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propDetails);
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
                    WriteNull();
                    break;
                case string strValue:
                    WriteValue(strValue);
                    break;
                case char ch:
                    WriteValue(ch);
                    break;
                case bool b:
                    WriteValue(b);
                    break;
                case LazyStringValue lazyStringValue:
                    _manualBlittableJsonDocumentBuilder.WriteValue(lazyStringValue);
                    break;
                case short s:
                    WriteValue(s);
                    break;
                case int i:
                    WriteValue(i);
                    break;
                case long l:
                    WriteValue(l);
                    break;
                case ushort us:
                    WriteValue(us);
                    break;
                case uint ui:
                    WriteValue(ui);
                    break;
                case ulong ul:
                    WriteValue(ul);
                    break;
                case double d:
                    WriteValue(d);
                    break;
                case decimal decVal:
                    WriteValue(decVal);
                    break;
                case float f:
                    WriteValue(f);
                    break;
                case byte b:
                    WriteValue(b);
                    break;
                case sbyte sb:
                    WriteValue(sb);
                    break;
                case LazyNumberValue lazyNumber:
                    _manualBlittableJsonDocumentBuilder.WriteValue(lazyNumber);
                    break;
                case DateTime dt:
                    WriteValue(dt);
                    break;
                case DateTimeOffset dto:
                    WriteValue(dto);
                    break;
                case TimeSpan ts:
                    WriteValue(ts);
                    break;
                case IDictionary<string, string> dics:
                    WriteDictionary(dics);
                    break;
                case IDictionary<string, object> dico:
                    WriteDictionary(dico);
                    break;
                case IDictionary iDictionary:
                    WriteDictionary(iDictionary);
                    break;
                case DynamicJsonValue val:
                    _manualBlittableJsonDocumentBuilder.StartWriteObject();
                    foreach (var prop in val.Properties)
                    {
                        _manualBlittableJsonDocumentBuilder.WritePropertyName(prop.Name);
                        WritePropertyValue(prop.Name, prop.Value);
                    }
                    _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
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

        private void WriteDictionary(IDictionary iDictionary)
        {
            _manualBlittableJsonDocumentBuilder.StartWriteObject();
            foreach (DictionaryEntry item in iDictionary)
            {
                var key = item.Key.ToString();
                _manualBlittableJsonDocumentBuilder.WritePropertyName(key);
                WritePropertyValue(key, item.Value);
            }
            _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
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

        public void WriteValue(LazyCompressedStringValue value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(LazyStringValue value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(LazyNumberValue value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(short value)
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

        public override void WriteValue(byte value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(decimal value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(DateTime dt)
        {
            var value = dt.GetDefaultRavenFormat();
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(DateTimeOffset dto)
        {
            var value = dto.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
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
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.GetDefaultRavenFormat());
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

        public override void WriteValue(TimeSpan ts)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(ts.ToString("c"));
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
