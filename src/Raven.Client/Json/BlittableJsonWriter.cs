using System;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;
using Sparrow.Extensions;
using Sparrow.Json;

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
            if (!_first) return;
            _first = false;
            WriteMetadata();
        }

        private void WriteMetadata()
        {
            if (_documentInfo == null) return;
            if (_documentInfo.Metadata?.Modifications != null && (_documentInfo.Metadata.Modifications.Properties.Count > 0))
            {
                _manualBlittableJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Key);
                _manualBlittableJsonDocumentBuilder.StartWriteObject();

                foreach (var prop in _documentInfo.Metadata.Modifications.Properties)
                {
                    if(prop.Item1.StartsWith("@") && prop.Item1 != Constants.Documents.Metadata.Collection)
                        continue; // system property, ignoring it
                    _manualBlittableJsonDocumentBuilder.WritePropertyName(prop.Item1);
                    var value = prop.Item2;

                    if (value == null)
                    {
                        _manualBlittableJsonDocumentBuilder.WriteValueNull();
                        continue;
                    }

                    var strValue = value as string;
                    if (strValue != null)
                        _manualBlittableJsonDocumentBuilder.WriteValue(strValue);
                    else if (value is long)
                        _manualBlittableJsonDocumentBuilder.WriteValue((long)value);
                    else if (value is double)
                        _manualBlittableJsonDocumentBuilder.WriteValue((double)value);
                    else if (value is decimal)
                        _manualBlittableJsonDocumentBuilder.WriteValue((decimal)value);
                    else if (value is float)
                        _manualBlittableJsonDocumentBuilder.WriteValue((float)value);
                    else if (value is bool)
                        _manualBlittableJsonDocumentBuilder.WriteValue((bool)value);
                    else if (value is LazyDoubleValue)
                        _manualBlittableJsonDocumentBuilder.WriteValue((LazyDoubleValue) value);
                    else
                        throw new NotSupportedException($"The value type {value.GetType().FullName} of key {prop.Item1} is not supported in the metadata");
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

                    switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
                    {
                        case BlittableJsonToken.StartArray:
                            _manualBlittableJsonDocumentBuilder.StartWriteArray();
                            var array = propertyDetails.Value as BlittableJsonReaderArray;
                            if (array != null)
                            {
                                var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                                foreach (BlittableJsonReaderObject entry in array)
                                {
                                    _manualBlittableJsonDocumentBuilder.StartWriteObject();
                                    var propsIndexes = entry.GetPropertiesByInsertionOrder();
                                    foreach (var index in propsIndexes)
                                    {
                                        entry.GetPropertyByIndex(index, ref propDetails);
                                        _manualBlittableJsonDocumentBuilder.WritePropertyName(propDetails.Name);
                                        switch (propDetails.Token)
                                        {
                                            case BlittableJsonToken.Integer:
                                                _manualBlittableJsonDocumentBuilder.WriteValue((long)propDetails.Value);
                                                break;
                                            case BlittableJsonToken.String:
                                                _manualBlittableJsonDocumentBuilder.WriteValue(propDetails.Value.ToString());
                                                break;
                                            default:
                                                throw new NotSupportedException($"Found property token of type '{propDetails.Token}' which is not supported.");
                                        }
                                    }
                                    _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
                                }
                            }
                            _manualBlittableJsonDocumentBuilder.WriteArrayEnd();
                            break;
                        case BlittableJsonToken.Integer:
                            _manualBlittableJsonDocumentBuilder.WriteValue((long) propertyDetails.Value);
                            break;
                        case BlittableJsonToken.Float:
                            _manualBlittableJsonDocumentBuilder.WriteValue((float)propertyDetails.Value);
                            break;
                        case BlittableJsonToken.String:
                            _manualBlittableJsonDocumentBuilder.WriteValue(propertyDetails.Value.ToString());
                            break;
                        case BlittableJsonToken.CompressedString:
                            _manualBlittableJsonDocumentBuilder.WriteValue(propertyDetails.Value.ToString());
                            break;
                        case BlittableJsonToken.Boolean:
                            _manualBlittableJsonDocumentBuilder.WriteValue((bool)propertyDetails.Value);
                            break;
                        case BlittableJsonToken.Null:
                            _manualBlittableJsonDocumentBuilder.WriteValueNull();
                            break;
                        default:
                            throw new NotSupportedException();
                    }
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
            if (double.IsNaN(value))
            {
                _manualBlittableJsonDocumentBuilder.WriteValue("NaN");
                return;
            }

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
            var s = value.ToString(Default.DateTimeOffsetFormatsToWrite);
            _manualBlittableJsonDocumentBuilder.WriteValue(s);
        }

        public override void WriteValue(int? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(long? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(float? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(double? value)
        {
            if (value != null)
            {
                if (double.IsNaN(value.Value))
                {
                    _manualBlittableJsonDocumentBuilder.WriteValue("NaN");
                    return;
                }

                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            }
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(bool? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(short? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(byte? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(decimal? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue((float)value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(DateTime? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.GetDefaultRavenFormat(isUtc: value.Value.Kind == DateTimeKind.Utc));
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(DateTimeOffset? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString(Default.DateTimeOffsetFormatsToWrite));
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
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
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString());
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(char value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public override void WriteValue(char? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString());
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(sbyte value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(sbyte? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(uint value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(uint? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(ushort value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(ushort? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(ulong value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue((long)value);
        }

        public override void WriteValue(ulong? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue((long)value.Value);
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(TimeSpan value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public override void WriteValue(TimeSpan? value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(byte[] value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(Convert.ToBase64String(value));
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(Uri value)
        {
            if (value != null) _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
            else _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(object value)
        {
            throw new NotSupportedException();
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