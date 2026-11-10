using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal interface IMetadataValueSink
    {
        void WriteNull();

        void WriteString(string value);

        void WriteString(LazyStringValue value);

        void WriteString(LazyCompressedStringValue value);

        void WriteChar(char value);

        void WriteBoolean(bool value);

        void WriteNumber(long value);

        void WriteNumber(ulong value);

        void WriteNumber(double value);

        void WriteNumber(float value);

        void WriteNumber(decimal value);

        void WriteNumber(LazyNumberValue value);

        void WriteStartObject();

        void WritePropertyName(string name);

        void WritePropertyName(LazyStringValue name);

        void WriteEndObject();

        void WriteStartArray();

        void WriteEndArray();
    }

    internal static class MetadataValueWriter
    {
        public static void Write<TSink>(ref TSink sink, string key, object value)
            where TSink : struct, IMetadataValueSink
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();

            switch (value)
            {
                case null:
                    sink.WriteNull();
                    break;
                case string s:
                    sink.WriteString(s);
                    break;
                case LazyStringValue s:
                    sink.WriteString(s);
                    break;
                case LazyCompressedStringValue s:
                    sink.WriteString(s);
                    break;
                case char c:
                    sink.WriteChar(c);
                    break;
                case bool b:
                    sink.WriteBoolean(b);
                    break;
                case long l:
                    sink.WriteNumber(l);
                    break;
                case int i:
                    sink.WriteNumber(i);
                    break;
                case short s:
                    sink.WriteNumber(s);
                    break;
                case byte b:
                    sink.WriteNumber(b);
                    break;
                case sbyte sb:
                    sink.WriteNumber(sb);
                    break;
                case ushort us:
                    sink.WriteNumber(us);
                    break;
                case uint ui:
                    sink.WriteNumber(ui);
                    break;
                case ulong ul:
                    sink.WriteNumber(ul);
                    break;
                case double d:
                    sink.WriteNumber(d);
                    break;
                case float f:
                    sink.WriteNumber(f);
                    break;
                case decimal dec:
                    sink.WriteNumber(dec);
                    break;
                case LazyNumberValue ln:
                    sink.WriteNumber(ln);
                    break;
                case DateTime dt:
                    sink.WriteString(dt.GetDefaultRavenFormat(isUtc: dt.Kind == DateTimeKind.Utc));
                    break;
                case DateTimeOffset dto:
                    sink.WriteString(dto.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
                    break;
                case TimeSpan ts:
                    sink.WriteString(ts.ToString("c"));
                    break;
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                case DateOnly @do:
                    sink.WriteString(@do.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture));
                    break;
                case TimeOnly to:
                    sink.WriteString(to.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture));
                    break;
#endif
                case IDictionary<string, string> dictionary:
                    sink.WriteStartObject();
                    foreach (KeyValuePair<string, string> entry in dictionary)
                    {
                        sink.WritePropertyName(entry.Key);
                        sink.WriteString(entry.Value);
                    }
                    sink.WriteEndObject();
                    break;
                case IDictionary<string, object> dictionary:
                    WriteDictionary(ref sink, dictionary);
                    break;
                case IDictionary dictionary:
                    sink.WriteStartObject();
                    foreach (DictionaryEntry entry in dictionary)
                    {
                        string entryKey = entry.Key.ToString();
                        sink.WritePropertyName(entryKey);
                        Write(ref sink, entryKey, entry.Value);
                    }
                    sink.WriteEndObject();
                    break;
                case DynamicJsonValue dynamicValue:
                    sink.WriteStartObject();
                    foreach ((string name, object propertyValue) in dynamicValue.Properties)
                    {
                        sink.WritePropertyName(name);
                        Write(ref sink, name, propertyValue);
                    }
                    sink.WriteEndObject();
                    break;
                case BlittableJsonReaderObject json:
                    WriteBlittable(ref sink, json);
                    break;
                case IEnumerable items:
                    sink.WriteStartArray();
                    foreach (object item in items)
                        Write(ref sink, key, item);
                    sink.WriteEndArray();
                    break;
                default:
                    throw new NotSupportedException($"The value type {value.GetType().FullName} of key {key} is not supported in the metadata");
            }
        }

        public static void WriteBlittable<TSink>(ref TSink sink, BlittableJsonReaderObject json)
            where TSink : struct, IMetadataValueSink
        {
            var property = new BlittableJsonReaderObject.PropertyDetails();
            sink.WriteStartObject();
            for (int i = 0; i < json.Count; i++)
            {
                json.GetPropertyByIndex(i, ref property);
                sink.WritePropertyName(property.Name);
                Write(ref sink, property.Name, property.Value);
            }
            sink.WriteEndObject();
        }

        public static void WriteDictionary<TSink>(ref TSink sink, IEnumerable<KeyValuePair<string, object>> dictionary)
            where TSink : struct, IMetadataValueSink
        {
            sink.WriteStartObject();
            foreach (KeyValuePair<string, object> entry in dictionary)
            {
                sink.WritePropertyName(entry.Key);
                Write(ref sink, entry.Key, entry.Value);
            }
            sink.WriteEndObject();
        }
    }

    internal readonly struct Utf8JsonMetadataValueSink(Utf8JsonWriter writer) : IMetadataValueSink
    {
        public void WriteNull() => writer.WriteNullValue();

        public void WriteString(string value) => writer.WriteStringValue(value);

        public unsafe void WriteString(LazyStringValue value) => writer.WriteStringValue(new ReadOnlySpan<byte>(value.Buffer, value.Size));

        public void WriteString(LazyCompressedStringValue value) => writer.WriteStringValue(value.ToString());

        public void WriteChar(char value)
        {
            Span<char> chars = stackalloc char[1];
            chars[0] = value;
            writer.WriteStringValue(chars);
        }

        public void WriteBoolean(bool value) => writer.WriteBooleanValue(value);

        public void WriteNumber(long value) => writer.WriteNumberValue(value);

        public void WriteNumber(ulong value) => writer.WriteNumberValue(value);

        public void WriteNumber(double value)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
                writer.WriteRawValue(value.ToString(CultureInfo.InvariantCulture), skipInputValidation: true);
            else
                writer.WriteNumberValue(value);
        }

        public void WriteNumber(float value)
        {
            if (float.IsNaN(value) || float.IsInfinity(value))
                writer.WriteRawValue(value.ToString(CultureInfo.InvariantCulture), skipInputValidation: true);
            else
                writer.WriteNumberValue(value);
        }

        public void WriteNumber(decimal value) => writer.WriteNumberValue(value);

        public void WriteNumber(LazyNumberValue value) => writer.WriteRawValue(value.ToString(), skipInputValidation: true);

        public void WriteStartObject() => writer.WriteStartObject();

        public void WritePropertyName(string name) => writer.WritePropertyName(name);

        public unsafe void WritePropertyName(LazyStringValue name) => writer.WritePropertyName(new ReadOnlySpan<byte>(name.Buffer, name.Size));

        public void WriteEndObject() => writer.WriteEndObject();

        public void WriteStartArray() => writer.WriteStartArray();

        public void WriteEndArray() => writer.WriteEndArray();
    }

    internal readonly struct BlittableMetadataValueSink(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> builder) : IMetadataValueSink
    {
        public void WriteNull() => builder.WriteValueNull();

        public void WriteString(string value)
        {
            if (value == null)
                builder.WriteValueNull();
            else
                builder.WriteValue(value);
        }

        public void WriteString(LazyStringValue value) => builder.WriteValue(value);

        public void WriteString(LazyCompressedStringValue value) => builder.WriteValue(value);

        public unsafe void WriteChar(char value)
        {
            Span<byte> utf8 = stackalloc byte[4];
            int length;
            fixed (byte* bytes = utf8)
                length = Encoding.UTF8.GetBytes(&value, 1, bytes, utf8.Length);

            builder.WriteValue(utf8.Slice(0, length));
        }

        public void WriteBoolean(bool value) => builder.WriteValue(value);

        public void WriteNumber(long value) => builder.WriteValue(value);

        public void WriteNumber(ulong value) => builder.WriteValue(value);

        public void WriteNumber(double value) => builder.WriteValue(value);

        public void WriteNumber(float value) => builder.WriteValue(value);

        public void WriteNumber(decimal value) => builder.WriteValue(value);

        public void WriteNumber(LazyNumberValue value) => builder.WriteValue(value);

        public void WriteStartObject() => builder.StartWriteObject();

        public void WritePropertyName(string name) => builder.WritePropertyName(name);

        public void WritePropertyName(LazyStringValue name) => builder.WritePropertyName(name);

        public void WriteEndObject() => builder.WriteObjectEnd();

        public void WriteStartArray() => builder.StartWriteArray();

        public void WriteEndArray() => builder.WriteArrayEnd();
    }
}
