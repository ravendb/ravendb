using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class DateTimeISO8601Converter : JsonConverterFactory
    {
        public static readonly DateTimeISO8601Converter Instance = new();

        private static readonly JsonConverter<DateTime?> NullableDateTime = new NullableConverter<DateTime>(DateTimeConverterInner.Instance);

        private static readonly JsonConverter<DateTimeOffset?> NullableDateTimeOffset = new NullableConverter<DateTimeOffset>(DateTimeOffsetConverterInner.Instance);

        private DateTimeISO8601Converter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert == typeof(DateTime)
                || typeToConvert == typeof(DateTime?)
                || typeToConvert == typeof(DateTimeOffset)
                || typeToConvert == typeof(DateTimeOffset?);
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            if (typeToConvert == typeof(DateTime))
                return DateTimeConverterInner.Instance;
            if (typeToConvert == typeof(DateTime?))
                return NullableDateTime;
            if (typeToConvert == typeof(DateTimeOffset))
                return DateTimeOffsetConverterInner.Instance;
            return NullableDateTimeOffset;
        }

        private static void ThrowUnparseable(string value, Type type) =>
            throw new JsonException($"The JSON value '{value}' could not be converted to {type}.");

        private static DateTime ReadDateTimeKey(string value)
        {
            if (DateTime.TryParseExact(value, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime key) == false)
                throw new JsonException($"Could not parse date time from '{value}'");

            return key.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(key, DateTimeKind.Local) : key;
        }

        private static string FormatDateTimeKey(DateTime value)
        {
            if (value.Kind == DateTimeKind.Unspecified)
                value = DateTime.SpecifyKind(value, DateTimeKind.Local);

            return value.GetDefaultRavenFormat();
        }

        private static DateTimeOffset ReadDateTimeOffsetKey(string value)
        {
            if (DateTimeOffset.TryParseExact(value, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset key) == false)
                throw new JsonException($"Could not parse date time offset from '{value}'");

            return key;
        }

        private static string FormatDateTimeOffsetKey(DateTimeOffset value)
        {
            return value.Offset == TimeSpan.Zero
                ? value.UtcDateTime.GetDefaultRavenFormat(isUtc: true)
                : value.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture);
        }

        private sealed class DateTimeConverterInner : JsonConverter<DateTime>
        {
            public static readonly DateTimeConverterInner Instance = new();

            public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                string s = reader.GetString();
                if (s != null)
                {
                    if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime time))
                    {
                        if (s.EndsWith("+00:00", StringComparison.Ordinal))
                            return time.ToUniversalTime();
                        return time;
                    }

                    DateTime lucene = LuceneDateTimeConverter.TryParseLucene(s);
                    if (lucene != default)
                        return DateTime.SpecifyKind(lucene, DateTimeKind.Local);

                    ThrowUnparseable(s, typeToConvert);
                }

                return default;
            }

            public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
            {
                if (value.Kind == DateTimeKind.Unspecified)
                    value = DateTime.SpecifyKind(value, DateTimeKind.Local);
                writer.WriteStringValue(value.GetDefaultRavenFormat());
            }

            public override DateTime ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return ReadDateTimeKey(reader.GetString());
            }

            public override void WriteAsPropertyName(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
            {
                writer.WritePropertyName(FormatDateTimeKey(value));
            }
        }

        private sealed class DateTimeOffsetConverterInner : JsonConverter<DateTimeOffset>
        {
            public static readonly DateTimeOffsetConverterInner Instance = new();

            public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                string s = reader.GetString();
                if (s != null)
                {
                    if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset time))
                        return time;

                    DateTime lucene = LuceneDateTimeConverter.TryParseLucene(s);
                    if (lucene != default)
                        return new DateTimeOffset(lucene, DateTimeOffset.Now.Offset);

                    ThrowUnparseable(s, typeToConvert);
                }

                return default;
            }

            public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(value.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
            }

            public override DateTimeOffset ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return ReadDateTimeOffsetKey(reader.GetString());
            }

            public override void WriteAsPropertyName(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
            {
                writer.WritePropertyName(FormatDateTimeOffsetKey(value));
            }
        }

        private sealed class NullableConverter<T>(JsonConverter<T> inner) : JsonConverter<T?>
            where T : struct
        {
            public override T? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return reader.TokenType == JsonTokenType.Null ? null : inner.Read(ref reader, typeof(T), options);
            }

            public override void Write(Utf8JsonWriter writer, T? value, JsonSerializerOptions options)
            {
                if (value == null)
                    writer.WriteNullValue();
                else
                    inner.Write(writer, value.Value, options);
            }

            public override T? ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return inner.ReadAsPropertyName(ref reader, typeof(T), options);
            }

            public override void WriteAsPropertyName(Utf8JsonWriter writer, T? value, JsonSerializerOptions options)
            {
                inner.WriteAsPropertyName(writer, value.Value, options);
            }
        }
    }
}
