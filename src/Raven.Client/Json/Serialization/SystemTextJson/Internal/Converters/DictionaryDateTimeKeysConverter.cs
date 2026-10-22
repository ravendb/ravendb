using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class DictionaryDateTimeKeysConverter : JsonConverterFactory
    {
        public static readonly DictionaryDateTimeKeysConverter Instance = new();

        private DictionaryDateTimeKeysConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            if (typeToConvert.IsGenericType == false)
                return false;

            Type genericDef = typeToConvert.GetGenericTypeDefinition();
            if (genericDef != typeof(Dictionary<,>) && genericDef != typeof(IDictionary<,>))
                return false;

            Type keyType = typeToConvert.GetGenericArguments()[0];
            return keyType == typeof(DateTime)
                || keyType == typeof(DateTime?)
                || keyType == typeof(DateTimeOffset)
                || keyType == typeof(DateTimeOffset?);
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            Type[] args = typeToConvert.GetGenericArguments();
            Type keyType = args[0];
            Type valueType = args[1];

            Type converterType;
            if (keyType == typeof(DateTime))
                converterType = typeof(DictionaryDateTimeKeysConverterInner<>).MakeGenericType(valueType);
            else if (keyType == typeof(DateTime?))
                converterType = typeof(DictionaryNullableDateTimeKeysConverterInner<>).MakeGenericType(valueType);
            else if (keyType == typeof(DateTimeOffset))
                converterType = typeof(DictionaryDateTimeOffsetKeysConverterInner<>).MakeGenericType(valueType);
            else
                converterType = typeof(DictionaryNullableDateTimeOffsetKeysConverterInner<>).MakeGenericType(valueType);

            return (JsonConverter)Activator.CreateInstance(converterType);
        }

        private sealed class DictionaryDateTimeKeysConverterInner<TValue> : JsonConverter<Dictionary<DateTime, TValue>>
        {
            public override Dictionary<DateTime, TValue> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartObject)
                    throw new JsonException($"Expected StartObject, got {reader.TokenType}");

                Dictionary<DateTime, TValue> result = new();
                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndObject)
                        return result;

                    if (reader.TokenType != JsonTokenType.PropertyName)
                        throw new JsonException($"Expected PropertyName, got {reader.TokenType}");

                    string s = reader.GetString();
                    if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime key) == false)
                        throw new JsonException($"Could not parse date time from '{s}'");

                    if (key.Kind == DateTimeKind.Unspecified)
                        key = DateTime.SpecifyKind(key, DateTimeKind.Local);

                    reader.Read();
                    TValue value = JsonSerializer.Deserialize<TValue>(ref reader, options);
                    result[key] = value;
                }

                throw new JsonException("Unexpected end of JSON.");
            }

            public override void Write(Utf8JsonWriter writer, Dictionary<DateTime, TValue> value, JsonSerializerOptions options)
            {
                writer.WriteStartObject();
                foreach (KeyValuePair<DateTime, TValue> kvp in value)
                {
                    DateTime dt = kvp.Key;
                    if (dt.Kind == DateTimeKind.Unspecified)
                        dt = DateTime.SpecifyKind(dt, DateTimeKind.Local);
                    writer.WritePropertyName(dt.GetDefaultRavenFormat());
                    JsonSerializer.Serialize(writer, kvp.Value, options);
                }
                writer.WriteEndObject();
            }
        }

        private sealed class DictionaryNullableDateTimeKeysConverterInner<TValue> : JsonConverter<Dictionary<DateTime?, TValue>>
        {
            public override Dictionary<DateTime?, TValue> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartObject)
                    throw new JsonException($"Expected StartObject, got {reader.TokenType}");

                Dictionary<DateTime?, TValue> result = new();
                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndObject)
                        return result;

                    if (reader.TokenType != JsonTokenType.PropertyName)
                        throw new JsonException($"Expected PropertyName, got {reader.TokenType}");

                    string s = reader.GetString();
                    DateTime? key;
                    if (s == null)
                    {
                        key = null;
                    }
                    else
                    {
                        if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime parsed) == false)
                            throw new JsonException($"Could not parse date time from '{s}'");

                        key = parsed.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(parsed, DateTimeKind.Local) : parsed;
                    }

                    reader.Read();
                    TValue value = JsonSerializer.Deserialize<TValue>(ref reader, options);
                    result[key] = value;
                }

                throw new JsonException("Unexpected end of JSON.");
            }

            public override void Write(Utf8JsonWriter writer, Dictionary<DateTime?, TValue> value, JsonSerializerOptions options)
            {
                writer.WriteStartObject();
                foreach (KeyValuePair<DateTime?, TValue> kvp in value)
                {
                    if (kvp.Key == null)
                        throw new ArgumentException($"Cannot serialize a null DateTime key in a dictionary.");

                    DateTime dt = kvp.Key.Value;
                    if (dt.Kind == DateTimeKind.Unspecified)
                        dt = DateTime.SpecifyKind(dt, DateTimeKind.Local);
                    writer.WritePropertyName(dt.GetDefaultRavenFormat());
                    JsonSerializer.Serialize(writer, kvp.Value, options);
                }
                writer.WriteEndObject();
            }
        }

        private sealed class DictionaryDateTimeOffsetKeysConverterInner<TValue> : JsonConverter<Dictionary<DateTimeOffset, TValue>>
        {
            public override Dictionary<DateTimeOffset, TValue> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartObject)
                    throw new JsonException($"Expected StartObject, got {reader.TokenType}");

                Dictionary<DateTimeOffset, TValue> result = new();
                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndObject)
                        return result;

                    if (reader.TokenType != JsonTokenType.PropertyName)
                        throw new JsonException($"Expected PropertyName, got {reader.TokenType}");

                    string s = reader.GetString();
                    if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset key) == false)
                        throw new JsonException($"Could not parse date time offset from '{s}'");

                    reader.Read();
                    TValue value = JsonSerializer.Deserialize<TValue>(ref reader, options);
                    result[key] = value;
                }

                throw new JsonException("Unexpected end of JSON.");
            }

            public override void Write(Utf8JsonWriter writer, Dictionary<DateTimeOffset, TValue> value, JsonSerializerOptions options)
            {
                writer.WriteStartObject();
                foreach (KeyValuePair<DateTimeOffset, TValue> kvp in value)
                {
                    DateTimeOffset dto = kvp.Key;
                    string keyStr = dto.Offset == TimeSpan.Zero
                        ? dto.UtcDateTime.GetDefaultRavenFormat(isUtc: true)
                        : dto.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture);
                    writer.WritePropertyName(keyStr);
                    JsonSerializer.Serialize(writer, kvp.Value, options);
                }
                writer.WriteEndObject();
            }
        }

        private sealed class DictionaryNullableDateTimeOffsetKeysConverterInner<TValue> : JsonConverter<Dictionary<DateTimeOffset?, TValue>>
        {
            public override Dictionary<DateTimeOffset?, TValue> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartObject)
                    throw new JsonException($"Expected StartObject, got {reader.TokenType}");

                Dictionary<DateTimeOffset?, TValue> result = new();
                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndObject)
                        return result;

                    if (reader.TokenType != JsonTokenType.PropertyName)
                        throw new JsonException($"Expected PropertyName, got {reader.TokenType}");

                    string s = reader.GetString();
                    DateTimeOffset? key;
                    if (s == null)
                    {
                        key = null;
                    }
                    else
                    {
                        if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset parsed) == false)
                            throw new JsonException($"Could not parse date time offset from '{s}'");
                        key = parsed;
                    }

                    reader.Read();
                    TValue value = JsonSerializer.Deserialize<TValue>(ref reader, options);
                    result[key] = value;
                }

                throw new JsonException("Unexpected end of JSON.");
            }

            public override void Write(Utf8JsonWriter writer, Dictionary<DateTimeOffset?, TValue> value, JsonSerializerOptions options)
            {
                writer.WriteStartObject();
                foreach (KeyValuePair<DateTimeOffset?, TValue> kvp in value)
                {
                    if (kvp.Key == null)
                        throw new ArgumentException($"Cannot serialize a null DateTimeOffset key in a dictionary.");

                    DateTimeOffset dto = kvp.Key.Value;
                    string keyStr = dto.Offset == TimeSpan.Zero
                        ? dto.UtcDateTime.GetDefaultRavenFormat(isUtc: true)
                        : dto.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture);
                    writer.WritePropertyName(keyStr);
                    JsonSerializer.Serialize(writer, kvp.Value, options);
                }
                writer.WriteEndObject();
            }
        }
    }
}
