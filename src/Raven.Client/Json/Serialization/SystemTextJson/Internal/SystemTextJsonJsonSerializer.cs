using System;
using System.Text.Json;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonJsonSerializer : IJsonSerializer
    {
        public JsonSerializerOptions Options { get; }

        public SystemTextJsonJsonSerializer(JsonSerializerOptions options)
        {
            Options = options;
        }

        public void Serialize(IJsonWriter writer, object value)
        {
            Serialize(writer, value, value?.GetType() ?? typeof(object));
        }

        public void Serialize(IJsonWriter writer, object value, Type objectType)
        {
            if (writer is SystemTextJsonBlittableWriter stjWriter)
            {
                // Serialize entity to UTF-8 in context native memory
                using var bufferWriter = new ContextMemoryBufferWriter(stjWriter.Context);
                using (var utf8Writer = new Utf8JsonWriter((System.Buffers.IBufferWriter<byte>)bufferWriter))
                {
                    JsonSerializer.Serialize(utf8Writer, value, objectType, Options);
                }
                WriteJsonToBlittableWriter(writer, bufferWriter.WrittenSpan);
            }
            else
            {
                var bytes = JsonSerializer.SerializeToUtf8Bytes(value, objectType, Options);
                WriteJsonToBlittableWriter(writer, bytes);
            }
        }

        /// <summary>
        /// Serialize entity to UTF-8 bytes in context native memory, then parse directly into
        /// a BlittableJsonReaderObject using the context's own parser. Bypasses the IJsonWriter
        /// token walk entirely.
        /// </summary>
        internal unsafe BlittableJsonReaderObject SerializeToBlittable(object value, Type objectType, JsonOperationContext context)
        {
            using var bufferWriter = new ContextMemoryBufferWriter(context);
            using (var utf8Writer = new Utf8JsonWriter((System.Buffers.IBufferWriter<byte>)bufferWriter))
            {
                JsonSerializer.Serialize(utf8Writer, value, objectType, Options);
            }

            return ParseBufferToBlittable(bufferWriter, context);
        }

        /// <summary>
        /// Serialize entity with @metadata prefix to UTF-8 bytes, then parse into blittable.
        /// Same approach as WriteEntityToStream but targets a context buffer instead of HTTP stream.
        /// </summary>
        internal unsafe BlittableJsonReaderObject SerializeToBlittableWithMetadata(object value, Type objectType,
            IMetadataDictionary metadata, JsonOperationContext context)
        {
            using var bufferWriter = new ContextMemoryBufferWriter(context);
            using (var utf8Writer = new Utf8JsonWriter((System.Buffers.IBufferWriter<byte>)bufferWriter, new System.Text.Json.JsonWriterOptions { SkipValidation = true }))
            {
                utf8Writer.WriteStartObject();

                // Write @metadata first
                if (metadata != null && metadata.Count > 0)
                {
                    utf8Writer.WritePropertyName(Constants.Documents.Metadata.Key);
                    utf8Writer.WriteStartObject();
                    foreach (var kvp in metadata)
                    {
                        utf8Writer.WritePropertyName(kvp.Key);
                        SystemTextJsonSerializationConventions.WriteMetadataValue(utf8Writer, kvp.Value);
                    }
                    utf8Writer.WriteEndObject();
                }

                // Serialize entity properties into the same object
                using var doc = JsonDocument.Parse(JsonSerializer.SerializeToUtf8Bytes(value, objectType, Options));
                foreach (var property in doc.RootElement.EnumerateObject())
                {
                    if (property.Name == Constants.Documents.Metadata.Key)
                        continue;
                    property.WriteTo(utf8Writer);
                }

                utf8Writer.WriteEndObject();
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

        private static void WriteJsonToBlittableWriter(IJsonWriter writer, ReadOnlySpan<byte> utf8Json)
        {
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
                        if (jsonReader.TryGetInt64(out long l)) writer.WriteValue(l);
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
