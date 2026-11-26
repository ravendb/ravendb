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
            return SerializeToBuffer(value, objectType, metadata: null, documentInfo: null, context);
        }

        internal ContextMemoryBufferWriter SerializeToBuffer(object value, Type objectType, IMetadataDictionary metadata, DocumentInfo documentInfo, JsonOperationContext context)
        {
            var buffer = new ContextMemoryBufferWriter(context);
            try
            {
                bool hasMetadata = documentInfo != null ? WriteMetadata(buffer, documentInfo) : WriteMetadata(buffer, metadata);
                int entityStart = buffer.WrittenCount;

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

                if (hasMetadata)
                    JoinEntityToMetadata(buffer, entityStart, objectType);

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
            using ContextMemoryBufferWriter buffer = SerializeToBuffer(value, objectType, metadata, documentInfo, context);
            return ParseBufferToBlittable(buffer, context);
        }

        private static bool WriteMetadata(ContextMemoryBufferWriter buffer, IMetadataDictionary metadata)
        {
            if (metadata == null || metadata.Count == 0)
                return false;

            Utf8JsonWriter metadataWriter = OpenMetadata(buffer);
            try
            {
                SystemTextJsonSerializationConventions.WriteMetadata(metadataWriter, metadata);
                metadataWriter.Flush();
            }
            finally
            {
                ReturnWriter(ref _metadataWriter, metadataWriter);
            }

            return true;
        }

        private static bool WriteMetadata(ContextMemoryBufferWriter bufferWriter, DocumentInfo documentInfo)
        {
            DynamicJsonValue modifications = documentInfo.Metadata?.Modifications;
            bool hasModifications = modifications != null && modifications.Properties.Count > 0;
            if (hasModifications == false && documentInfo.Metadata == null && documentInfo.MetadataInstance == null && documentInfo.Collection == null)
                return false;

            Utf8JsonWriter metadataWriter = OpenMetadata(bufferWriter);
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

            return true;
        }

        private static void JoinEntityToMetadata(ContextMemoryBufferWriter bufferWriter, int entityStart, Type objectType)
        {
            ReadOnlySpan<byte> written = bufferWriter.WrittenSpan;
            int entityLength = written.Length - entityStart;
            if (entityLength < 2 || written[entityStart] != '{' || written[written.Length - 1] != '}')
                throw new InvalidOperationException($"Entity of type '{objectType}' must serialize to a JSON object.");

            if (entityLength == 2)
            {
                bufferWriter.Truncate(2);
                bufferWriter.WriteByte((byte)'}');
                return;
            }

            bufferWriter.Overwrite(entityStart, (byte)',');
        }

        private static Utf8JsonWriter OpenMetadata(ContextMemoryBufferWriter bufferWriter)
        {
            bufferWriter.WriteByte((byte)'{');

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

        private void WriteJsonToBlittableWriter(SystemTextJsonBlittableWriter writer, ReadOnlySpan<byte> utf8Json)
        {
            var jsonReader = new Utf8JsonReader(utf8Json, new JsonReaderOptions { MaxDepth = Options.MaxDepth });
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
                        else WriteNonIntegerNumber(writer, ref jsonReader);
                        break;
                    case JsonTokenType.True: writer.WriteValue(true); break;
                    case JsonTokenType.False: writer.WriteValue(false); break;
                    case JsonTokenType.Null: writer.WriteNull(); break;
                }
            }
        }

        private const int MaxExactDecimalDigits = 28;

        private const int MaxExactDoubleDigits = 15;

        private static void WriteNonIntegerNumber(SystemTextJsonBlittableWriter writer, ref Utf8JsonReader jsonReader)
        {
            ReadOnlySpan<byte> number = jsonReader.ValueSpan;
            int exponent = number.IndexOfAny((byte)'e', (byte)'E');
            int digits = CountDigits(exponent < 0 ? number : number.Slice(0, exponent));

            if (exponent < 0 && digits <= MaxExactDecimalDigits && jsonReader.TryGetDecimal(out decimal dec))
                writer.WriteValue(dec);
            else if (exponent >= 0 && digits <= MaxExactDoubleDigits)
                writer.WriteValue(jsonReader.GetDouble());
            else
                writer.WriteNumber(number);
        }

        private static int CountDigits(ReadOnlySpan<byte> number)
        {
            int digits = 0;
            for (int i = 0; i < number.Length; i++)
            {
                if (number[i] >= (byte)'0' && number[i] <= (byte)'9')
                    digits++;
            }

            return digits;
        }
    }
}
