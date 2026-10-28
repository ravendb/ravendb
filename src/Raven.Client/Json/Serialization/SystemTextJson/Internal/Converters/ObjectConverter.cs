using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class ObjectConverter : JsonConverter<object>
    {
        private readonly TypeNameHandling _typeNameHandling;

        public ObjectConverter(TypeNameHandling typeNameHandling)
        {
            _typeNameHandling = typeNameHandling;
        }

        public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (_typeNameHandling != TypeNameHandling.None && TypeNameConverter.TryReadTypedObject(ref reader, typeof(object), options, out object typed))
                return typed;

            return reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray
                ? JsonDynamicConverters.ReadDocument(ref reader)
                : JsonDynamicConverters.ReadPrimitive(ref reader);
        }

        public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
        {
            Type runtimeType = value.GetType();
            if (runtimeType == typeof(object))
            {
                writer.WriteStartObject();
                writer.WriteEndObject();
                return;
            }

            if (_typeNameHandling == TypeNameHandling.None)
            {
                JsonSerializer.Serialize(writer, value, runtimeType, options);
                return;
            }

            TypeNameConverter.WriteValue(writer, value, typeof(object), _typeNameHandling == TypeNameHandling.Objects, options);
        }
    }
}
