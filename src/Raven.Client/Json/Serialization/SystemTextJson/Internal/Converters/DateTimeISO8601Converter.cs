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
                return NullableDateTimeConverterInner.Instance;
            if (typeToConvert == typeof(DateTimeOffset))
                return DateTimeOffsetConverterInner.Instance;
            return NullableDateTimeOffsetConverterInner.Instance;
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
                        if (s.EndsWith("+00:00"))
                            return time.ToUniversalTime();
                        return time;
                    }

                    DateTime lucene = LuceneDateTimeConverter.TryParseLucene(s, isOffset: false);
                    if (lucene != default)
                        return DateTime.SpecifyKind(lucene, DateTimeKind.Local);
                }

                return default;
            }

            public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
            {
                if (value.Kind == DateTimeKind.Unspecified)
                    value = DateTime.SpecifyKind(value, DateTimeKind.Local);
                writer.WriteStringValue(value.GetDefaultRavenFormat());
            }
        }

        private sealed class NullableDateTimeConverterInner : JsonConverter<DateTime?>
        {
            public static readonly NullableDateTimeConverterInner Instance = new();

            public override DateTime? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                string s = reader.GetString();
                if (s != null)
                {
                    if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime time))
                    {
                        if (s.EndsWith("+00:00"))
                            return time.ToUniversalTime();
                        return time;
                    }

                    DateTime lucene = LuceneDateTimeConverter.TryParseLucene(s, isOffset: false);
                    if (lucene != default)
                        return DateTime.SpecifyKind(lucene, DateTimeKind.Local);
                }

                return null;
            }

            public override void Write(Utf8JsonWriter writer, DateTime? value, JsonSerializerOptions options)
            {
                if (value == null)
                {
                    writer.WriteNullValue();
                    return;
                }

                DateTime dt = value.Value;
                if (dt.Kind == DateTimeKind.Unspecified)
                    dt = DateTime.SpecifyKind(dt, DateTimeKind.Local);
                writer.WriteStringValue(dt.GetDefaultRavenFormat());
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

                    DateTime lucene = LuceneDateTimeConverter.TryParseLucene(s, isOffset: true);
                    if (lucene != default)
                        return new DateTimeOffset(lucene, DateTimeOffset.Now.Offset);
                }

                return default;
            }

            public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(value.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
            }
        }

        private sealed class NullableDateTimeOffsetConverterInner : JsonConverter<DateTimeOffset?>
        {
            public static readonly NullableDateTimeOffsetConverterInner Instance = new();

            public override DateTimeOffset? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                string s = reader.GetString();
                if (s != null)
                {
                    if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset time))
                        return time;

                    DateTime lucene = LuceneDateTimeConverter.TryParseLucene(s, isOffset: true);
                    if (lucene != default)
                        return new DateTimeOffset(lucene, DateTimeOffset.Now.Offset);
                }

                return null;
            }

            public override void Write(Utf8JsonWriter writer, DateTimeOffset? value, JsonSerializerOptions options)
            {
                if (value == null)
                {
                    writer.WriteNullValue();
                    return;
                }

                writer.WriteStringValue(value.Value.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
            }
        }
    }
}
