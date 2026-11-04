using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class IntegerEnumConverter : JsonConverterFactory
    {
        public static readonly IntegerEnumConverter Instance = new();

        private IntegerEnumConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert.IsEnum;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            return (JsonConverter)Activator.CreateInstance(typeof(IntegerEnumConverterInner<>).MakeGenericType(typeToConvert));
        }

        private sealed class IntegerEnumConverterInner<T> : JsonConverter<T>
            where T : struct, Enum
        {
            private readonly Dictionary<string, T> _enumMemberValues = new(StringComparer.OrdinalIgnoreCase);

            private readonly bool _unsigned;

            public IntegerEnumConverterInner()
            {
                foreach (KeyValuePair<string, string> entry in EnumMemberStringEnumConverter.GetEnumMemberValues(typeof(T)))
                    _enumMemberValues[entry.Value] = (T)Enum.Parse(typeof(T), entry.Key);

                _unsigned = Type.GetTypeCode(Enum.GetUnderlyingType(typeof(T))) is TypeCode.Byte or TypeCode.UInt16 or TypeCode.UInt32 or TypeCode.UInt64;
            }

            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                switch (reader.TokenType)
                {
                    case JsonTokenType.Number:
                        return (T)Enum.ToObject(typeof(T), _unsigned ? reader.GetUInt64() : reader.GetInt64());
                    case JsonTokenType.String:
                        return Parse(reader.GetString());
                    default:
                        throw new JsonException($"Expected a number or a string for '{typeof(T).FullName}' but got {reader.TokenType}.");
                }
            }

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                if (_unsigned)
                    writer.WriteNumberValue(Convert.ToUInt64(value));
                else
                    writer.WriteNumberValue(Convert.ToInt64(value));
            }

            public override T ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return Parse(reader.GetString());
            }

            public override void WriteAsPropertyName(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                writer.WritePropertyName(value.ToString());
            }

            private T Parse(string text)
            {
                if (_enumMemberValues.TryGetValue(text, out T value) || Enum.TryParse(text, ignoreCase: true, out value))
                    return value;

                throw new JsonException($"The JSON value '{text}' could not be converted to {typeof(T)}.");
            }
        }
    }
}
