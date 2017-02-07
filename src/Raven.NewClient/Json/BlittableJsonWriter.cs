using System;
using Newtonsoft.Json;
using Raven.NewClient.Abstractions.Data;
using Sparrow.Json;
using Raven.NewClient.Client.Document;
using Raven.Abstractions.Extensions;
using Raven.NewClient.Abstractions;

namespace Raven.NewClient.Client.Json
{
    public class BlittableJsonWriter : JsonWriter
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
                _manualBlittalbeJsonDocumentBuilder.WritePropertyName(Constants.Metadata.Key);
                _manualBlittalbeJsonDocumentBuilder.StartWriteObject();

                foreach (var prop in _documentInfo.Metadata.Modifications.Properties)
                {
                    _manualBlittalbeJsonDocumentBuilder.WritePropertyName(prop.Item1);
                    _manualBlittalbeJsonDocumentBuilder.WriteValue(prop.Item2.ToString());
                }

                _manualBlittalbeJsonDocumentBuilder.WriteObjectEnd();
                _documentInfo.Metadata.Modifications = null;
            }
            else if (_documentInfo.Metadata != null)
            {
                _manualBlittalbeJsonDocumentBuilder.WritePropertyName(Constants.Metadata.Key);
                _manualBlittalbeJsonDocumentBuilder.StartWriteObject();
                var ids = _documentInfo.Metadata.GetPropertiesByInsertionOrder();

                foreach (var id in ids)
                {
                    var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                    _documentInfo.Metadata.GetPropertyByIndex(id, ref propertyDetails);

                    _manualBlittalbeJsonDocumentBuilder.WritePropertyName(propertyDetails.Name);
                    switch (propertyDetails.Token)
                    {
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
            if (value != null) _manualBlittalbeJsonDocumentBuilder.WriteValue(value.Value);
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
            throw new NotSupportedException();
        }

        public override void WriteValue(ulong? value)
        {
            throw new NotSupportedException();
        }

        public override void WriteValue(TimeSpan value)
        {
            throw new NotSupportedException();
        }

        public override void WriteValue(TimeSpan? value)
        {
            throw new NotSupportedException();
        }

        public override void WriteValue(byte[] value)
        {
            throw new NotSupportedException();
        }

        public override void WriteValue(Uri value)
        {
            throw new NotSupportedException();
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