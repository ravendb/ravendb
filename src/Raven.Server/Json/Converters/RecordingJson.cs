using System;
using System.IO;
using System.Runtime.Serialization;
using System.Text.Json;
using Raven.Client.Json.Serialization.SystemTextJson.Internal;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Server.Json.Converters
{
    internal static class RecordingJson
    {
        private const string ArrayPropertyName = "Items";

        [ThreadStatic]
        private static JsonOperationContext _context;

        public static JsonOperationContext Context =>
            _context ?? SystemTextJsonBlittableReader.Current ?? throw new SerializationException($"A {nameof(JsonOperationContext)} must be entered before reading recorded values");

        public static ContextScope EnterContext(JsonOperationContext context)
        {
            JsonOperationContext previous = _context;
            _context = context;
            return new ContextScope(previous);
        }

        public readonly struct ContextScope(JsonOperationContext previous) : IDisposable
        {
            public void Dispose() => _context = previous;
        }

        public static void Write(Utf8JsonWriter writer, object value)
        {
            switch (value)
            {
                case null:
                    writer.WriteNullValue();
                    return;
                case BlittableJsonReaderObject blittable:
                    Write(writer, blittable);
                    return;
                case BlittableJsonReaderArray array:
                    writer.WriteStartArray();
                    foreach (object item in array)
                        Write(writer, item);
                    writer.WriteEndArray();
                    return;
                case LazyStringValue lazyString:
                    writer.WriteStringValue(lazyString.ToString());
                    return;
                case LazyCompressedStringValue lazyCompressedString:
                    writer.WriteStringValue(lazyCompressedString.ToString());
                    return;
                case LazyNumberValue lazyNumber:
                    if (NamedFloatingPointLiterals.TryWriteEscaped(writer, lazyNumber) == false)
                        writer.WriteRawValue(lazyNumber.Inner.ToString(), skipInputValidation: true);
                    return;
                case string text:
                    writer.WriteStringValue(text);
                    return;
                case bool boolean:
                    writer.WriteBooleanValue(boolean);
                    return;
                case long number:
                    writer.WriteNumberValue(number);
                    return;
                case double number:
                    if (double.IsFinite(number))
                        writer.WriteNumberValue(number);
                    else
                        NamedFloatingPointLiterals.Write(writer, number);
                    return;
                case BlittableJsonReaderVector vector:
                    Write(writer, vector);
                    return;
                case BlittableJsonReaderObject.RawBlob blob:
                    Write(writer, blob);
                    return;
                default:
                    throw new SerializationException($"Cannot record a blittable value of type {value.GetType()}");
            }
        }

        private static void Write(Utf8JsonWriter writer, BlittableJsonReaderObject blittable)
        {
            JsonOperationContext context = _context ?? blittable._context;
            BlittableJsonReaderObject materialized = blittable.Modifications != null
                ? context.ReadObject(blittable, "recording/modifications")
                : null;

            using (materialized)
            using (var stream = new MemoryStream())
            {
                using (var textWriter = new BlittableJsonTextWriter(context, stream) { NamedFloatingPointLiteralFormat = NamedFloatingPointLiterals.BlittableFormat })
                    (materialized ?? blittable).WriteJsonTo(textWriter);

                writer.WriteRawValue(stream.GetBuffer().AsSpan(0, (int)stream.Length), skipInputValidation: true);
            }
        }

        private static void Write(Utf8JsonWriter writer, BlittableJsonReaderVector vector)
        {
            using (var stream = new MemoryStream())
            {
                using (var textWriter = new BlittableJsonTextWriter(_context ?? vector._context, stream) { NamedFloatingPointLiteralFormat = NamedFloatingPointLiterals.BlittableFormat })
                    textWriter.WriteValue(BlittableJsonToken.Vector, vector);

                writer.WriteRawValue(stream.GetBuffer().AsSpan(0, (int)stream.Length), skipInputValidation: true);
            }
        }

        private static unsafe void Write(Utf8JsonWriter writer, BlittableJsonReaderObject.RawBlob blob)
        {
            writer.WriteRawValue(new ReadOnlySpan<byte>(blob.Address, blob.Length), skipInputValidation: true);
        }

        public static BlittableJsonReaderObject ReadObject(ref Utf8JsonReader reader)
        {
            using (var writer = new SystemTextJsonBlittableWriter(Context))
            {
                CopyValue(ref reader, writer);
                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }

        public static BlittableJsonReaderArray ReadArray(ref Utf8JsonReader reader)
        {
            using (var writer = new SystemTextJsonBlittableWriter(Context))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(ArrayPropertyName);
                CopyValue(ref reader, writer);
                writer.WriteEndObject();
                writer.FinalizeDocument();

                BlittableJsonReaderObject parent = writer.CreateReader();
                parent.TryGet(ArrayPropertyName, out BlittableJsonReaderArray array);
                return array;
            }
        }

        private static void CopyValue(ref Utf8JsonReader reader, SystemTextJsonBlittableWriter writer)
        {
            int depth = reader.CurrentDepth;
            bool isContainer = reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray;

            CopyToken(ref reader, writer);
            if (isContainer == false)
                return;

            while (reader.Read())
            {
                CopyToken(ref reader, writer);
                if (reader.CurrentDepth == depth)
                    return;
            }

            throw new SerializationException("Recorded value ended before its closing token");
        }

        private static void CopyToken(ref Utf8JsonReader reader, SystemTextJsonBlittableWriter writer)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    writer.WriteStartObject();
                    break;
                case JsonTokenType.EndObject:
                    writer.WriteEndObject();
                    break;
                case JsonTokenType.StartArray:
                    writer.WriteStartArray();
                    break;
                case JsonTokenType.EndArray:
                    writer.WriteEndArray();
                    break;
                case JsonTokenType.PropertyName:
                    writer.WritePropertyName(reader.GetString());
                    break;
                case JsonTokenType.String:
                    if (NamedFloatingPointLiterals.TryRead(ref reader, out double literal))
                        writer.WriteValue(literal);
                    else
                        writer.WriteValue(reader.GetString());
                    break;
                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out long integer))
                        writer.WriteValue(integer);
                    else
                        writer.WriteNumber(reader.ValueSpan);
                    break;
                case JsonTokenType.True:
                    writer.WriteValue(true);
                    break;
                case JsonTokenType.False:
                    writer.WriteValue(false);
                    break;
                case JsonTokenType.Null:
                    writer.WriteNull();
                    break;
            }
        }
    }
}
