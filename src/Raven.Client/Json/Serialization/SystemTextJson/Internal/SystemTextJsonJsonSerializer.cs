using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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
            var stjWriter = (SystemTextJsonBlittableWriter)writer;
            using (ContextMemoryBufferWriter buffer = SerializeEscapedToBuffer(value, objectType, stjWriter.Context))
                WriteJsonToBlittableWriter(stjWriter, buffer.WrittenSpan);
        }

        private ContextMemoryBufferWriter SerializeEscapedToBuffer(object value, Type objectType, JsonOperationContext context)
        {
            using (NamedFloatingPointLiterals.WriteEscaped())
                return SerializeToBuffer(value, objectType, context);
        }

        private void SerializeValue(Utf8JsonWriter writer, object value, Type objectType)
        {
            try
            {
                if (value != null && _typeNameHandling != TypeNameHandling.None && value.GetType() != objectType)
                    TypeNameConverter.WriteValue(writer, value, objectType, alwaysWriteObjectTypeName: _typeNameHandling == TypeNameHandling.Objects, Options);
                else if (value != null && _typeNameHandling == TypeNameHandling.None)
                    TypeNameConverter.WriteUntyped(writer, value, Options);
                else
                    JsonSerializer.Serialize(writer, value, objectType, Options);
            }
            finally
            {
                RavenJsonTypeInfoResolver.ClearSerializing();
            }
        }

        internal ContextMemoryBufferWriter SerializeToBuffer(object value, Type objectType, JsonOperationContext context)
        {
            var buffer = new ContextMemoryBufferWriter(context);
            try
            {
                Utf8JsonWriter writer = RentWriter(ref _entityWriter, buffer, SystemTextJsonSerializationConventions.WriterOptions);
                try
                {
                    SerializeValue(writer, value, objectType);
                    writer.Flush();
                }
                finally
                {
                    ReturnWriter(ref _entityWriter, writer);
                }

                return buffer;
            }
            catch
            {
                buffer.Dispose();
                throw;
            }
        }

        [ThreadStatic]
        private static Utf8JsonWriter _entityWriter;

        [ThreadStatic]
        private static Utf8JsonWriter _metadataWriter;

        private static Utf8JsonWriter RentWriter(ref Utf8JsonWriter cached, IBufferWriter<byte> buffer, JsonWriterOptions options)
        {
            Utf8JsonWriter writer = cached;
            if (writer == null)
                return new Utf8JsonWriter(buffer, options);

            cached = null;
            writer.Reset(buffer);
            return writer;
        }

        private static void ReturnWriter(ref Utf8JsonWriter cached, Utf8JsonWriter writer)
        {
            writer.Reset(DetachedOutput.Instance);
            cached = writer;
        }

        private sealed class DetachedOutput : IBufferWriter<byte>
        {
            public static readonly DetachedOutput Instance = new();

            private readonly byte[] _buffer = new byte[256];

            public void Advance(int count)
            {
            }

            public Memory<byte> GetMemory(int sizeHint = 0) => sizeHint <= _buffer.Length ? _buffer : new byte[sizeHint];

            public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
        }

        internal BlittableJsonReaderObject SerializeToBlittable(object value, Type objectType, IMetadataDictionary metadata, DocumentInfo documentInfo, JsonOperationContext context)
        {
            using ContextMemoryBufferWriter buffer = SerializeToBuffer(value, objectType, context);
            if (documentInfo != null)
                AppendMetadata(buffer, objectType, documentInfo);
            else
                AppendMetadata(buffer, objectType, metadata);

            return ParseBufferToBlittable(buffer, context);
        }

        internal static void AppendMetadata(ContextMemoryBufferWriter buffer, Type objectType, IMetadataDictionary metadata)
        {
            if (metadata == null || metadata.Count == 0)
                return;

            Utf8JsonWriter metadataWriter = OpenMetadata(buffer, objectType);
            try
            {
                SystemTextJsonSerializationConventions.WriteMetadata(metadataWriter, metadata);
                metadataWriter.Flush();
            }
            finally
            {
                ReturnWriter(ref _metadataWriter, metadataWriter);
            }

            buffer.WriteByte((byte)'}');
        }

        private static void AppendMetadata(ContextMemoryBufferWriter bufferWriter, Type objectType, DocumentInfo documentInfo)
        {
            DynamicJsonValue modifications = documentInfo.Metadata?.Modifications;
            bool hasModifications = modifications != null && modifications.Properties.Count > 0;
            if (hasModifications == false && documentInfo.Metadata == null && documentInfo.MetadataInstance == null && documentInfo.Collection == null)
                return;

            Utf8JsonWriter metadataWriter = OpenMetadata(bufferWriter, objectType);
            try
            {
                var sink = new Utf8JsonMetadataValueSink(metadataWriter);
                if (hasModifications)
                {
                    sink.WriteStartObject();
                    foreach ((string name, object propertyValue) in modifications.Properties)
                    {
                        sink.WritePropertyName(name);
                        MetadataValueWriter.Write(ref sink, name, propertyValue);
                    }

                    if (documentInfo.Collection != null)
                    {
                        sink.WritePropertyName(Constants.Documents.Metadata.Collection);
                        sink.WriteString(documentInfo.Collection);
                    }

                    sink.WriteEndObject();
                    documentInfo.Metadata.Modifications = null;
                }
                else if (documentInfo.Metadata != null)
                {
                    MetadataValueWriter.WriteBlittable(ref sink, documentInfo.Metadata);
                }
                else if (documentInfo.MetadataInstance != null)
                {
                    MetadataValueWriter.WriteDictionary(ref sink, documentInfo.MetadataInstance);
                }
                else
                {
                    sink.WriteStartObject();
                    sink.WritePropertyName(Constants.Documents.Metadata.Collection);
                    sink.WriteString(documentInfo.Collection);
                    if (documentInfo.Id != null)
                    {
                        sink.WritePropertyName(Constants.Documents.Metadata.Id);
                        sink.WriteString(documentInfo.Id);
                    }

                    sink.WriteEndObject();
                }

                metadataWriter.Flush();
            }
            finally
            {
                ReturnWriter(ref _metadataWriter, metadataWriter);
            }

            bufferWriter.WriteByte((byte)'}');
        }

        private static Utf8JsonWriter OpenMetadata(ContextMemoryBufferWriter bufferWriter, Type objectType)
        {
            ReadOnlySpan<byte> written = bufferWriter.WrittenSpan;
            if (written.Length < 2 || written[written.Length - 1] != '}')
                throw new InvalidOperationException($"Entity of type '{objectType}' must serialize to a JSON object.");

            bool hasProperties = written.Length > 2;
            bufferWriter.Truncate(1);
            if (hasProperties)
                bufferWriter.WriteByte((byte)',');

            Utf8JsonWriter metadataWriter = RentWriter(ref _metadataWriter, bufferWriter, SystemTextJsonSerializationConventions.MetadataWriterOptions);
            metadataWriter.WritePropertyName(Constants.Documents.Metadata.Key);
            return metadataWriter;
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

        private static void WriteJsonToBlittableWriter(SystemTextJsonBlittableWriter writer, ReadOnlySpan<byte> utf8Json)
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
                    case JsonTokenType.PropertyName:
                        if (jsonReader.ValueIsEscaped || jsonReader.ValueSpan.IsEmpty) writer.WritePropertyName(jsonReader.GetString());
                        else writer.WritePropertyName(jsonReader.ValueSpan);
                        break;
                    case JsonTokenType.String:
                        if (NamedFloatingPointLiterals.TryRead(ref jsonReader, out double literal)) writer.WriteValue(literal);
                        else if (jsonReader.ValueIsEscaped) writer.WriteValue(jsonReader.GetString());
                        else writer.WriteValue(jsonReader.ValueSpan);
                        break;
                    case JsonTokenType.Number:
                        if (jsonReader.TryGetInt64(out long l)) writer.WriteValue(l);
                        else if (jsonReader.ValueSpan.IndexOfAny((byte)'e', (byte)'E') < 0 && jsonReader.TryGetDecimal(out decimal dec)) writer.WriteValue(dec);
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
