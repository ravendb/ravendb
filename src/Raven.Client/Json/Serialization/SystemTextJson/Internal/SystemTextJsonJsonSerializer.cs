using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonJsonSerializer : IJsonSerializer
    {
        private readonly TypeNameHandling _typeNameHandling;

        public JsonSerializerOptions Options { get; }

        public SystemTextJsonJsonSerializer(JsonSerializerOptions options, TypeNameHandling typeNameHandling)
        {
            Options = options;
            _typeNameHandling = typeNameHandling;
        }

        public void Serialize(IJsonWriter writer, object value)
        {
            Serialize(writer, value, value?.GetType() ?? typeof(object));
        }

        public void Serialize(IJsonWriter writer, object value, Type objectType)
        {
            if (writer is SystemTextJsonBlittableWriter stjWriter)
            {
                using var bufferWriter = new ContextMemoryBufferWriter(stjWriter.Context);
                using (var utf8Writer = new Utf8JsonWriter((IBufferWriter<byte>)bufferWriter, SystemTextJsonSerializationConventions.WriterOptions))
                {
                    SerializeValue(utf8Writer, value, objectType);
                }
                WriteJsonToBlittableWriter(writer, bufferWriter.WrittenSpan);
            }
            else
            {
                using var stream = new MemoryStream();
                using (var utf8Writer = new Utf8JsonWriter(stream, SystemTextJsonSerializationConventions.WriterOptions))
                {
                    SerializeValue(utf8Writer, value, objectType);
                }
                WriteJsonToBlittableWriter(writer, stream.ToArray());
            }
        }

        private void SerializeValue(Utf8JsonWriter writer, object value, Type objectType)
        {
            if (value != null && _typeNameHandling != TypeNameHandling.None && value.GetType() != objectType)
                TypeNameConverter.WriteValue(writer, value, objectType, alwaysWriteObjectTypeName: _typeNameHandling == TypeNameHandling.Objects, Options);
            else
                JsonSerializer.Serialize(writer, value, objectType, Options);
        }

        internal unsafe BlittableJsonReaderObject SerializeToBlittable(object value, Type objectType, JsonOperationContext context)
        {
            using var bufferWriter = new ContextMemoryBufferWriter(context);
            using (var utf8Writer = new Utf8JsonWriter((IBufferWriter<byte>)bufferWriter, SystemTextJsonSerializationConventions.WriterOptions))
            {
                JsonSerializer.Serialize(utf8Writer, value, objectType, Options);
            }

            return ParseBufferToBlittable(bufferWriter, context);
        }

        internal unsafe BlittableJsonReaderObject SerializeToBlittableWithMetadata(object value, Type objectType,
            IMetadataDictionary metadata, JsonOperationContext context)
        {
            using var bufferWriter = new ContextMemoryBufferWriter(context);
            using (var utf8Writer = new Utf8JsonWriter((IBufferWriter<byte>)bufferWriter, SystemTextJsonSerializationConventions.WriterOptions))
            {
                JsonSerializer.Serialize(utf8Writer, value, objectType, Options);
            }

            if (metadata != null && metadata.Count > 0)
            {
                ReadOnlySpan<byte> written = bufferWriter.WrittenSpan;
                if (written.Length < 2 || written[written.Length - 1] != '}')
                    throw new InvalidOperationException($"Entity of type '{objectType}' must serialize to a JSON object.");

                bool hasProperties = written.Length > 2;
                bufferWriter.Truncate(1);
                if (hasProperties)
                    bufferWriter.WriteByte((byte)',');

                using (var metadataWriter = new Utf8JsonWriter((IBufferWriter<byte>)bufferWriter, SystemTextJsonSerializationConventions.MetadataWriterOptions))
                {
                    metadataWriter.WritePropertyName(Constants.Documents.Metadata.Key);
                    SystemTextJsonSerializationConventions.WriteMetadata(metadataWriter, metadata);
                }

                bufferWriter.WriteByte((byte)'}');
            }

            return ParseBufferToBlittable(bufferWriter, context);
        }

        private static unsafe BlittableJsonReaderObject ParseBufferToBlittable(ContextMemoryBufferWriter bufferWriter, JsonOperationContext context)
        {
            var utf8Json = bufferWriter.WrittenSpan;
            fixed (byte* ptr = utf8Json)
            {
                return context.ParseBuffer(ptr, utf8Json.Length, "serialize/entity",
                    BlittableJsonDocumentBuilder.UsageMode.None);
            }
        }

        public object Deserialize(IJsonReader reader, Type type)
        {
            var stjReader = (SystemTextJsonBlittableReader)reader;
            return JsonSerializer.Deserialize(stjReader.GetUtf8Json(), type, Options);
        }

        public T Deserialize<T>(IJsonReader reader)
        {
            var stjReader = (SystemTextJsonBlittableReader)reader;
            return JsonSerializer.Deserialize<T>(stjReader.GetUtf8Json(), Options);
        }

        private static ReadOnlySpan<byte> NaN => "NaN"u8;

        private static ReadOnlySpan<byte> Infinity => "Infinity"u8;

        private static ReadOnlySpan<byte> NegativeInfinity => "-Infinity"u8;

        private static Dictionary<long, double> ReplaceNamedFloatingPointLiterals(byte[] json)
        {
            Dictionary<long, double> literals = null;
            bool inString = false;
            for (int i = 0; i < json.Length; i++)
            {
                byte current = json[i];
                if (inString)
                {
                    if (current == (byte)'\\')
                        i++;
                    else if (current == (byte)'"')
                        inString = false;
                    continue;
                }

                if (current == (byte)'"')
                {
                    inString = true;
                    continue;
                }

                ReadOnlySpan<byte> rest = json.AsSpan(i);
                if (rest.StartsWith(NegativeInfinity))
                    i = ReplaceLiteral(json, i, "-0.000000"u8, double.NegativeInfinity, ref literals);
                else if (rest.StartsWith(Infinity))
                    i = ReplaceLiteral(json, i, "0.000000"u8, double.PositiveInfinity, ref literals);
                else if (rest.StartsWith(NaN))
                    i = ReplaceLiteral(json, i, "0e0"u8, double.NaN, ref literals);
            }

            return literals;
        }

        private static int ReplaceLiteral(byte[] json, int index, ReadOnlySpan<byte> replacement, double value, ref Dictionary<long, double> literals)
        {
            replacement.CopyTo(json.AsSpan(index));
            (literals ??= new Dictionary<long, double>())[index] = value;
            return index + replacement.Length - 1;
        }

        private static void WriteJsonToBlittableWriter(IJsonWriter writer, ReadOnlySpan<byte> utf8Json)
        {
            Dictionary<long, double> namedLiterals = null;
            if (utf8Json.IndexOf(NaN) >= 0 || utf8Json.IndexOf(Infinity) >= 0)
            {
                byte[] copy = utf8Json.ToArray();
                namedLiterals = ReplaceNamedFloatingPointLiterals(copy);
                utf8Json = copy;
            }

            var jsonReader = new Utf8JsonReader(utf8Json);
            while (jsonReader.Read())
            {
                switch (jsonReader.TokenType)
                {
                    case JsonTokenType.StartObject: writer.WriteStartObject(); break;
                    case JsonTokenType.EndObject: writer.WriteEndObject(); break;
                    case JsonTokenType.StartArray: writer.WriteStartArray(); break;
                    case JsonTokenType.EndArray: writer.WriteEndArray(); break;
                    case JsonTokenType.PropertyName: writer.WritePropertyName(jsonReader.GetString()); break;
                    case JsonTokenType.String: writer.WriteValue(jsonReader.GetString()); break;
                    case JsonTokenType.Number:
                        if (namedLiterals != null && namedLiterals.TryGetValue(jsonReader.TokenStartIndex, out double literal)) writer.WriteValue(literal);
                        else if (jsonReader.TryGetInt64(out long l)) writer.WriteValue(l);
                        else if (jsonReader.TryGetDecimal(out decimal dec)) writer.WriteValue(dec);
                        else writer.WriteValue(jsonReader.GetDouble());
                        break;
                    case JsonTokenType.True: writer.WriteValue(true); break;
                    case JsonTokenType.False: writer.WriteValue(false); break;
                    case JsonTokenType.Null: writer.WriteNull(); break;
                }
            }
        }
    }
}
