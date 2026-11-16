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

        private const int SecondsPrecisionLength = 19;

        private const int MaxFractionDigits = 7;

        private const int DefaultRavenFormatUtcLength = 28;

        private const int RoundtripDateTimeOffsetLength = 33;

        private static void ThrowUnparseable(string value, Type type) =>
            throw new JsonException($"The JSON value '{value}' could not be converted to {type}.");

        private static bool TryReadWithoutOffset(ref Utf8JsonReader reader, out DateTime value)
        {
            value = default;
            if (reader.TokenType != JsonTokenType.String || reader.HasValueSequence)
                return false;

            ReadOnlySpan<byte> s = reader.ValueSpan;
            if (s.Length < SecondsPrecisionLength || s.Length > DefaultRavenFormatUtcLength)
                return false;

            DateTimeKind kind = DateTimeKind.Unspecified;
            if (s[s.Length - 1] == (byte)'Z')
            {
                kind = DateTimeKind.Utc;
                s = s.Slice(0, s.Length - 1);
            }

            if (s.Length < SecondsPrecisionLength)
                return false;

            if (s[4] != (byte)'-' || s[7] != (byte)'-' || s[10] != (byte)'T' || s[13] != (byte)':' || s[16] != (byte)':')
                return false;

            int fraction = 0;
            if (s.Length > SecondsPrecisionLength)
            {
                int fractionDigits = s.Length - SecondsPrecisionLength - 1;
                if (s[SecondsPrecisionLength] != (byte)'.' || fractionDigits < 1 || fractionDigits > MaxFractionDigits ||
                    TryReadDigits(s.Slice(SecondsPrecisionLength + 1), out fraction) == false)
                    return false;

                for (; fractionDigits < MaxFractionDigits; fractionDigits++)
                    fraction *= 10;
            }

            if (TryReadDigits(s.Slice(0, 4), out int year) == false ||
                TryReadDigits(s.Slice(5, 2), out int month) == false ||
                TryReadDigits(s.Slice(8, 2), out int day) == false ||
                TryReadDigits(s.Slice(11, 2), out int hour) == false ||
                TryReadDigits(s.Slice(14, 2), out int minute) == false ||
                TryReadDigits(s.Slice(17, 2), out int second) == false)
                return false;

            if (year == 0 || month == 0 || month > 12 || day == 0 || day > DateTime.DaysInMonth(year, month) || hour > 23 || minute > 59 || second > 59)
                return false;

            value = new DateTime(year, month, day, hour, minute, second, kind).AddTicks(fraction);
            return true;
        }

        private static bool TryReadDigits(ReadOnlySpan<byte> digits, out int value)
        {
            value = 0;
            for (int i = 0; i < digits.Length; i++)
            {
                int digit = digits[i] - '0';
                if ((uint)digit > 9)
                    return false;

                value = value * 10 + digit;
            }

            return true;
        }

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
                if (TryReadWithoutOffset(ref reader, out DateTime value))
                    return value;

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

            public override unsafe void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
            {
                byte* buffer = stackalloc byte[DefaultRavenFormatUtcLength];
                int size = value.GetDefaultRavenFormat(buffer, DefaultRavenFormatUtcLength, value.Kind == DateTimeKind.Utc);
                writer.WriteStringValue(new ReadOnlySpan<byte>(buffer, size));
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
#if NET8_0_OR_GREATER
                Span<byte> buffer = stackalloc byte[RoundtripDateTimeOffsetLength];
                if (value.TryFormat(buffer, out int written, DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture))
                {
                    writer.WriteStringValue(buffer.Slice(0, written));
                    return;
                }
#endif

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
