using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class EmptyStringNullableConverter : JsonConverterFactory
    {
        public static readonly EmptyStringNullableConverter Instance = new();

        private EmptyStringNullableConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            Type underlying = Nullable.GetUnderlyingType(typeToConvert);
            return underlying != null && (underlying.IsEnum || underlying == typeof(bool) || underlying == typeof(Guid) || underlying == typeof(TimeSpan));
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            Type underlying = Nullable.GetUnderlyingType(typeToConvert);
            return (JsonConverter)Activator.CreateInstance(typeof(Inner<>).MakeGenericType(underlying), [options.GetConverter(underlying)]);
        }

        private sealed class Inner<T>(JsonConverter inner) : JsonConverter<T?>
            where T : struct
        {
            private readonly JsonConverter<T> _inner = (JsonConverter<T>)inner;

            public override T? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.String && reader.HasValueSequence == false && reader.ValueSpan.IsEmpty)
                    return null;

                return _inner.Read(ref reader, typeof(T), options);
            }

            public override void Write(Utf8JsonWriter writer, T? value, JsonSerializerOptions options)
            {
                if (value == null)
                    writer.WriteNullValue();
                else
                    _inner.Write(writer, value.Value, options);
            }

            public override T? ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return _inner.ReadAsPropertyName(ref reader, typeof(T), options);
            }

            public override void WriteAsPropertyName(Utf8JsonWriter writer, T? value, JsonSerializerOptions options)
            {
                _inner.WriteAsPropertyName(writer, value.Value, options);
            }
        }
    }
}
