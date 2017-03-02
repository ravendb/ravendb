using System;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json
{
    internal class BlittableJsonWriter : JsonWriter
    {
        private readonly ManualBlittalbeJsonDocumentBuilder<UnmanagedWriteBuffer> _manualBlittalbeJsonDocumentBuilder;
        private bool _first;
        private readonly DocumentInfo _documentInfo;

        public BlittableJsonWriter(JsonOperationContext context, DocumentInfo documentInfo = null,
            BlittableJsonDocumentBuilder.UsageMode? mode = null, BlittableWriter<UnmanagedWriteBuffer> writer = null)
        {
            _manualBlittalbeJsonDocumentBuilder = new ManualBlittalbeJsonDocumentBuilder<UnmanagedWriteBuffer>(context, mode ?? BlittableJsonDocumentBuilder.UsageMode.None, writer);
            _manualBlittalbeJsonDocumentBuilder.Reset(mode ?? BlittableJsonDocumentBuilder.UsageMode.None);
            _manualBlittalbeJsonDocumentBuilder.StartWriteObjectDocument();
            _documentInfo = documentInfo;
            _first = true;
        }

        public override void WriteStartObject()
        {
            _manualBlittalbeJsonDocumentBuilder.StartWriteObject();
            if (!_first) return;
            _first = false;
            WriteMetadata();
        }

        private void WriteMetadata()
        {
            if (_documentInfo == null) return;
            if (_documentInfo.Metadata?.Modifications != null && (_documentInfo.Metadata.Modifications.Properties.Count > 0))
            {
                _manualBlittalbeJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Key);
                _manualBlittalbeJsonDocumentBuilder.StartWriteObject();

                foreach (var prop in _documentInfo.Metadata.Modifications.Properties)
                {
                    _manualBlittalbeJsonDocumentBuilder.WritePropertyName(prop.Item1);
                    var value = prop.Item2;

                    if (value == null)
                    {
                        _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
                        continue;
                    }

                    var strValue = value as string;
                    if (strValue != null)
                        _manualBlittalbeJsonDocumentBuilder.WriteValue(strValue);
                    else if (value is long)
                        _manualBlittalbeJsonDocumentBuilder.WriteValue((long)value);
                    else if (value is double)
                        _manualBlittalbeJsonDocumentBuilder.WriteValue((double)value);
                    else if (value is decimal)
                        _manualBlittalbeJsonDocumentBuilder.WriteValue((decimal)value);
                    else if (value is float)
                        _manualBlittalbeJsonDocumentBuilder.WriteValue((float)value);
                    else if (value is bool)
                        _manualBlittalbeJsonDocumentBuilder.WriteValue((bool)value);
                    else if (value is LazyDoubleValue)
                        _manualBlittalbeJsonDocumentBuilder.WriteValue((LazyDoubleValue) value);
                    else
                        throw new NotSupportedException($"The value type {value.GetType().FullName} of key {prop.Item1} is not supported in the metadata");
                }

                if (_documentInfo.Collection != null)
                {
                    _manualBlittalbeJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Collection);
                    _manualBlittalbeJsonDocumentBuilder.WriteValue(_documentInfo.Collection);
                }

                _manualBlittalbeJsonDocumentBuilder.WriteObjectEnd();
                _documentInfo.Metadata.Modifications = null;
            }
            else if (_documentInfo.Metadata != null)
            {
                _manualBlittalbeJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Key);
                _manualBlittalbeJsonDocumentBuilder.StartWriteObject();
                var ids = _documentInfo.Metadata.GetPropertiesByInsertionOrder();

                foreach (var id in ids)
                {
                    var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                    _documentInfo.Metadata.GetPropertyByIndex(id, ref propertyDetails);
                    _manualBlittalbeJsonDocumentBuilder.WritePropertyName(propertyDetails.Name);

                    switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
                    {
                        case BlittableJsonToken.StartArray:
                            //in this case it can only be change vector (since it is the only array in the metadata)
                            ThrowIfNotSupportedArrayProperty(propertyDetails);

                            _manualBlittalbeJsonDocumentBuilder.StartWriteArray();
                            var changeVectorArray = propertyDetails.Value as BlittableJsonReaderArray;
                            if (changeVectorArray != null)
                            {
                                var changeVectorEntryPropDetails = new BlittableJsonReaderObject.PropertyDetails();
                                foreach (BlittableJsonReaderObject entry in changeVectorArray)
                                {
                                    _manualBlittalbeJsonDocumentBuilder.StartWriteObject();
                                    var propsIndexes = entry.GetPropertiesByInsertionOrder();
                                    foreach (var index in propsIndexes)
                                    {
                                        entry.GetPropertyByIndex(index, ref changeVectorEntryPropDetails);
                                        _manualBlittalbeJsonDocumentBuilder.WritePropertyName(changeVectorEntryPropDetails.Name);
                                        switch (changeVectorEntryPropDetails.Token)
                                        {
                                            case BlittableJsonToken.Integer:
                                                _manualBlittalbeJsonDocumentBuilder.WriteValue((long)changeVectorEntryPropDetails.Value);
                                                break;
                                            case BlittableJsonToken.String:
                                                _manualBlittalbeJsonDocumentBuilder.WriteValue(changeVectorEntryPropDetails.Value.ToString());
                                                break;
                                        }

                                    }
                                    _manualBlittalbeJsonDocumentBuilder.WriteObjectEnd();
                                }
                            }
                            _manualBlittalbeJsonDocumentBuilder.WriteArrayEnd();
                            break;
                        case BlittableJsonToken.Integer:
                            _manualBlittalbeJsonDocumentBuilder.WriteValue((long) propertyDetails.Value);
                            break;
                        case BlittableJsonToken.Float:
                            _manualBlittalbeJsonDocumentBuilder.WriteValue((float) propertyDetails.Value);
                            break;
                        case BlittableJsonToken.String:
                            _manualBlittalbeJsonDocumentBuilder.WriteValue(propertyDetails.Value.ToString());
                            break;
                        case BlittableJsonToken.CompressedString:
                            _manualBlittalbeJsonDocumentBuilder.WriteValue(propertyDetails.Value.ToString());
                            break;
                        case BlittableJsonToken.Boolean:
                            _manualBlittalbeJsonDocumentBuilder.WriteValue((bool) propertyDetails.Value);
                            break;
                        case BlittableJsonToken.Null:
                            _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
                            break;
                        default:
                            throw new NotSupportedException();
                    }
                }
                _manualBlittalbeJsonDocumentBuilder.WriteObjectEnd();
            }
            else if (_documentInfo.Collection != null)
            {
                _manualBlittalbeJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Key);
                _manualBlittalbeJsonDocumentBuilder.StartWriteObject();

                _manualBlittalbeJsonDocumentBuilder.WritePropertyName(Constants.Documents.Metadata.Collection);
                _manualBlittalbeJsonDocumentBuilder.WriteValue(_documentInfo.Collection);

                _manualBlittalbeJsonDocumentBuilder.WriteObjectEnd();
            }
        }

        private static void ThrowIfNotSupportedArrayProperty(BlittableJsonReaderObject.PropertyDetails propertyDetails)
        {
            if (propertyDetails.Name != Constants.Documents.Metadata.ChangeVector)
            {
                throw new NotSupportedException("Expected to the array to be property called 'ChangeVector', but found " +
                                                propertyDetails.Name + ", this is not supported.");
            }
        }

        public override void WriteEndObject()
        {
            _manualBlittalbeJsonDocumentBuilder.WriteObjectEnd();
        }

        public void FinalizeDocument()
        {
            _manualBlittalbeJsonDocumentBuilder.FinalizeDocument();
        }

        public override void WriteStartArray()
        {
            _manualBlittalbeJsonDocumentBuilder.StartWriteArray();
        }

        public override void WriteEndArray()
        {
            _manualBlittalbeJsonDocumentBuilder.WriteArrayEnd();
        }

        public override void WritePropertyName(string name)
        {
            _manualBlittalbeJsonDocumentBuilder.WritePropertyName(name);
        }

        public override void WritePropertyName(string name, bool escape)
        {
            _manualBlittalbeJsonDocumentBuilder.WritePropertyName(name);
        }

        public override void WriteNull()
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(string value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(int value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(long value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(float value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(double value)
        {
            if (double.IsNaN(value))
            {
                _manualBlittalbeJsonDocumentBuilder.WriteValue("NaN");
                return;
            }

            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(bool value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(short value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(byte value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(decimal value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(DateTime value)
        {
            var s = value.GetDefaultRavenFormat(isUtc: value.Kind == DateTimeKind.Utc);
            _manualBlittalbeJsonDocumentBuilder.WriteValue(s);
        }

        public override void WriteValue(DateTimeOffset value)
        {
            var s = value.ToString(Default.DateTimeOffsetFormatsToWrite);
            _manualBlittalbeJsonDocumentBuilder.WriteValue(s);
        }

        public override void WriteValue(int? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(long? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(float? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(double? value)
        {
            if (value != null)
            {
                if (double.IsNaN(value.Value))
                {
                    _manualBlittalbeJsonDocumentBuilder.WriteValue("NaN");
                    return;
                }

                _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            }
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(bool? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(short? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(byte? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(decimal? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue((float)value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(DateTime? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value.GetDefaultRavenFormat(isUtc: value.Value.Kind == DateTimeKind.Utc));
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(DateTimeOffset? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value.ToString(Default.DateTimeOffsetFormatsToWrite));
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        protected override void Dispose(bool disposing)
        {
            _manualBlittalbeJsonDocumentBuilder.Dispose();
        }

        public BlittableJsonReaderObject CreateReader()
        {
            return _manualBlittalbeJsonDocumentBuilder.CreateReader();
        }

        public override void WriteValue(Guid value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public override void WriteValue(Guid? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value.ToString());
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(char value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public override void WriteValue(char? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value.ToString());
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(sbyte value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(sbyte? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(uint value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(uint? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(ushort value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value);
        }

        public override void WriteValue(ushort? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(ulong value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue((long)value);
        }

        public override void WriteValue(ulong? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue((long)value.Value);
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(TimeSpan value)
        {
            _manualBlittalbeJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public override void WriteValue(TimeSpan? value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.ToString());
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(byte[] value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(Convert.ToBase64String(value));
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
        }

        public override void WriteValue(Uri value)
        {
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.ToString());
            else _manualBlittalbeJsonDocumentBuilder.WriteValueNull();
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