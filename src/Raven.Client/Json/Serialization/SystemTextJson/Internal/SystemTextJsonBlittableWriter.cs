using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Session;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonBlittableWriter : IJsonWriter
    {
        private readonly ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> _manualBlittableJsonDocumentBuilder;

        public JsonOperationContext Context { get; }

        public SystemTextJsonBlittableWriter(JsonOperationContext context)
        {
            Context = context;
            _manualBlittableJsonDocumentBuilder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context, BlittableJsonDocumentBuilder.UsageMode.None);
            _manualBlittableJsonDocumentBuilder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
            _manualBlittableJsonDocumentBuilder.StartWriteObjectDocument();
        }

        public void WriteStartObject()
        {
            _manualBlittableJsonDocumentBuilder.StartWriteObject();
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

        private void WritePropertyValue(string propName, object value)
        {
            var sink = new BlittableMetadataValueSink(_manualBlittableJsonDocumentBuilder);
            MetadataValueWriter.Write(ref sink, propName, value);
        }

        public void WriteEndObject()
        {
            _manualBlittableJsonDocumentBuilder.WriteObjectEnd();
        }

        public void FinalizeDocument()
        {
            _manualBlittableJsonDocumentBuilder.FinalizeDocument();
        }

        public void WriteStartArray()
        {
            _manualBlittableJsonDocumentBuilder.StartWriteArray();
        }

        public void WriteEndArray()
        {
            _manualBlittableJsonDocumentBuilder.WriteArrayEnd();
        }

        public void WritePropertyName(string name)
        {
            _manualBlittableJsonDocumentBuilder.WritePropertyName(name);
        }

        public void WritePropertyName(string name, bool escape)
        {
            _manualBlittableJsonDocumentBuilder.WritePropertyName(name);
        }

        public void WriteNull()
        {
            _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(string value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(LazyCompressedStringValue value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(LazyStringValue value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(LazyNumberValue value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(short value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(int value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(long value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(float value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(double value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(bool value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(byte value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(decimal value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(DateTime dt)
        {
            string value = dt.GetDefaultRavenFormat();
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(DateTimeOffset dto)
        {
            string value = dto.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
        public void WriteValue(TimeOnly to)
        {
            string value = to.ToString(DefaultFormat.TimeOnlyFormatToWrite);
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(DateOnly @do)
        {
            string value = @do.ToString(DefaultFormat.DateOnlyFormatToWrite);
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(TimeOnly? to)
        {
            if (to != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(to.Value.ToString(DefaultFormat.TimeOnlyFormatToWrite));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(DateOnly? @do)
        {
            if (@do != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(@do.Value.ToString(DefaultFormat.DateOnlyFormatToWrite));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }
#endif

        public void WriteValue(int? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(long? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(float? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(double? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(bool? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(short? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(byte? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(decimal? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(DateTime? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.GetDefaultRavenFormat());
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(DateTimeOffset? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(Guid value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public void WriteValue(Guid? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString());
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(char value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
        }

        public void WriteValue(char? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString());
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(sbyte value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(sbyte? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(ushort value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(ushort? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(uint value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(uint? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(ulong value)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(value);
        }

        public void WriteValue(ulong? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value);
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(TimeSpan ts)
        {
            _manualBlittableJsonDocumentBuilder.WriteValue(ts.ToString("c"));
        }

        public void WriteValue(TimeSpan? value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.Value.ToString("c"));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(byte[] value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(Convert.ToBase64String(value));
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteValue(Uri value)
        {
            if (value != null)
                _manualBlittableJsonDocumentBuilder.WriteValue(value.ToString());
            else
                _manualBlittableJsonDocumentBuilder.WriteValueNull();
        }

        public void WriteVector<T>(ReadOnlySpan<T> value) where T : unmanaged
        {
            _manualBlittableJsonDocumentBuilder.WriteVector(value);
        }

        public void WriteValue(object value)
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
                        using (BlittableJsonReaderObject clonedBlittable = readerObject.CloneOnTheSameContext())
                        {
                            _manualBlittableJsonDocumentBuilder.WriteEmbeddedBlittableDocument(clonedBlittable);
                        }
                    }
                    return;
                case LazyStringValue lazyStringValue:
                    _manualBlittableJsonDocumentBuilder.WriteValue(lazyStringValue);
                    return;
                case null:
                    WriteNull();
                    return;
                case string strValue:
                    WriteValue(strValue);
                    return;
                case bool boolValue:
                    WriteValue(boolValue);
                    return;
                case int intValue:
                    WriteValue(intValue);
                    return;
                case long longValue:
                    WriteValue(longValue);
                    return;
                case double doubleValue:
                    WriteValue(doubleValue);
                    return;
                case float floatValue:
                    WriteValue(floatValue);
                    return;
                case decimal decimalValue:
                    WriteValue(decimalValue);
                    return;
                case DateTime dtValue:
                    WriteValue(dtValue);
                    return;
                case DateTimeOffset dtoValue:
                    WriteValue(dtoValue);
                    return;
                case Guid guidValue:
                    WriteValue(guidValue);
                    return;
                case TimeSpan tsValue:
                    WriteValue(tsValue);
                    return;
                case Uri uriValue:
                    WriteValue(uriValue);
                    return;
                case byte[] bytesValue:
                    WriteValue(bytesValue);
                    return;
                default:
                    throw new NotSupportedException($"The value type {value.GetType().FullName} is not supported");
            }
        }

        public BlittableJsonReaderObject CreateReader()
        {
            return _manualBlittableJsonDocumentBuilder.CreateReader();
        }

        public void Flush()
        {
            // nothing to do
        }

        public void Close()
        {
            throw new NotSupportedException();
        }

        public void WriteComment(string text)
        {
            throw new NotSupportedException();
        }

        public void WriteEnd()
        {
            throw new NotSupportedException();
        }

        public void WriteEndConstructor()
        {
            throw new NotSupportedException();
        }

        public void WriteStartConstructor(string name)
        {
            throw new NotSupportedException();
        }

        public void WriteUndefined()
        {
            throw new NotSupportedException();
        }

        public void WriteRaw(string json)
        {
            throw new NotSupportedException();
        }

        public void WriteRawValue(string json)
        {
            throw new NotSupportedException();
        }

        public void WriteWhitespace(string ws)
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
            _manualBlittableJsonDocumentBuilder.Dispose();
        }
    }
}
