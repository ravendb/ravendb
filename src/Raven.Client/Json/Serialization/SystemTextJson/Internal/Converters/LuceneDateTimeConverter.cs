using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    /// <summary>
    /// Converts Lucene date-time strings (17 numeric characters) to DateTime/DateTimeOffset.
    /// Writing is not supported; this converter is read-only and intended as a fallback
    /// when the primary ISO 8601 parse fails.
    /// </summary>
    internal sealed class LuceneDateTimeConverter : JsonConverterFactory
    {
        // 17 numeric characters on a datetime field == Lucene datetime
        private static readonly Regex LuceneDateTimePattern = new Regex(@"^\d{17}$", RegexOptions.Compiled);

        public static readonly LuceneDateTimeConverter Instance = new();

        private LuceneDateTimeConverter()
        {
        }

        /// <summary>
        /// Tries to parse a Lucene date string. Returns <see cref="DateTime.MinValue"/> if the input
        /// is not a 17-digit Lucene date string.
        /// </summary>
        internal static DateTime TryParseLucene(string input, bool isOffset)
        {
            if (input != null && LuceneDateTimePattern.IsMatch(input))
                return DateTools.StringToDate(input);

            return default;
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
                return LuceneDateTimeConverterInner.Instance;
            if (typeToConvert == typeof(DateTime?))
                return LuceneNullableDateTimeConverterInner.Instance;
            if (typeToConvert == typeof(DateTimeOffset))
                return LuceneDateTimeOffsetConverterInner.Instance;
            return LuceneNullableDateTimeOffsetConverterInner.Instance;
        }

        private sealed class LuceneDateTimeConverterInner : JsonConverter<DateTime>
        {
            public static readonly LuceneDateTimeConverterInner Instance = new();

            public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                string s = reader.GetString();
                DateTime lucene = TryParseLucene(s, isOffset: false);
                if (lucene != default)
                    return DateTime.SpecifyKind(lucene, DateTimeKind.Local);

                return default;
            }

            public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
            {
                throw new NotSupportedException($"{nameof(LuceneDateTimeConverter)} does not support writing.");
            }
        }

        private sealed class LuceneNullableDateTimeConverterInner : JsonConverter<DateTime?>
        {
            public static readonly LuceneNullableDateTimeConverterInner Instance = new();

            public override DateTime? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                string s = reader.GetString();
                DateTime lucene = TryParseLucene(s, isOffset: false);
                if (lucene != default)
                    return DateTime.SpecifyKind(lucene, DateTimeKind.Local);

                return null;
            }

            public override void Write(Utf8JsonWriter writer, DateTime? value, JsonSerializerOptions options)
            {
                throw new NotSupportedException($"{nameof(LuceneDateTimeConverter)} does not support writing.");
            }
        }

        private sealed class LuceneDateTimeOffsetConverterInner : JsonConverter<DateTimeOffset>
        {
            public static readonly LuceneDateTimeOffsetConverterInner Instance = new();

            public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                string s = reader.GetString();
                DateTime lucene = TryParseLucene(s, isOffset: true);
                if (lucene != default)
                    return new DateTimeOffset(lucene, DateTimeOffset.Now.Offset);

                return default;
            }

            public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
            {
                throw new NotSupportedException($"{nameof(LuceneDateTimeConverter)} does not support writing.");
            }
        }

        private sealed class LuceneNullableDateTimeOffsetConverterInner : JsonConverter<DateTimeOffset?>
        {
            public static readonly LuceneNullableDateTimeOffsetConverterInner Instance = new();

            public override DateTimeOffset? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                string s = reader.GetString();
                DateTime lucene = TryParseLucene(s, isOffset: true);
                if (lucene != default)
                    return new DateTimeOffset(lucene, DateTimeOffset.Now.Offset);

                return null;
            }

            public override void Write(Utf8JsonWriter writer, DateTimeOffset? value, JsonSerializerOptions options)
            {
                throw new NotSupportedException($"{nameof(LuceneDateTimeConverter)} does not support writing.");
            }
        }
    }
}
