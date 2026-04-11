#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    // ISO 8601 standard
    // More info available at: https://docs.microsoft.com/en-us/dotnet/standard/base-types/standard-date-and-time-format-strings#Roundtrip
    internal sealed class DateOnlyConverter : JsonConverterFactory
    {
        public static readonly DateOnlyConverter Instance = new();

        private DateOnlyConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert == typeof(DateOnly) || typeToConvert == typeof(DateOnly?);
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            if (typeToConvert == typeof(DateOnly))
                return DateOnlyConverterInner.Instance;

            return NullableDateOnlyConverter.Instance;
        }

        private sealed class DateOnlyConverterInner : JsonConverter<DateOnly>
        {
            public static readonly DateOnlyConverterInner Instance = new();

            public override unsafe DateOnly Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.String)
                    throw new InvalidOperationException("Expected string, Got " + reader.TokenType);

                string value = reader.GetString();

                fixed (char* buffer = value.AsSpan())
                {
                    if (LazyStringParser.TryParseDateOnly(buffer, value.Length, out DateOnly dateOnly) == false)
                    {
                        if (LazyStringParser.TryParseDateTime(buffer, value.Length, out DateTime dt, out _, true) is LazyStringParser.Result.Failed
                            or LazyStringParser.Result.DateTimeOffset)
                        {
                            throw new InvalidOperationException("Expected DateOnly, Got " + value);
                        }

                        return DateOnly.FromDateTime(dt);
                    }

                    return dateOnly;
                }
            }

            public override void Write(Utf8JsonWriter writer, DateOnly value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(value.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture));
            }
        }

        private sealed class NullableDateOnlyConverter : JsonConverter<DateOnly?>
        {
            public static readonly NullableDateOnlyConverter Instance = new();

            public override unsafe DateOnly? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                if (reader.TokenType != JsonTokenType.String)
                    throw new InvalidOperationException("Expected string, Got " + reader.TokenType);

                string value = reader.GetString();

                fixed (char* buffer = value.AsSpan())
                {
                    if (LazyStringParser.TryParseDateOnly(buffer, value.Length, out DateOnly dateOnly) == false)
                    {
                        if (LazyStringParser.TryParseDateTime(buffer, value.Length, out DateTime dt, out _, true) is LazyStringParser.Result.Failed
                            or LazyStringParser.Result.DateTimeOffset)
                        {
                            throw new InvalidOperationException("Expected DateOnly, Got " + value);
                        }

                        return DateOnly.FromDateTime(dt);
                    }

                    return dateOnly;
                }
            }

            public override void Write(Utf8JsonWriter writer, DateOnly? value, JsonSerializerOptions options)
            {
                if (value == null)
                {
                    writer.WriteNullValue();
                    return;
                }

                writer.WriteStringValue(value.Value.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture));
            }
        }
    }
}
#endif
