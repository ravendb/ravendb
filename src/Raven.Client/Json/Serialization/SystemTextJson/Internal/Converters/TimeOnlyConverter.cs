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
    internal sealed class TimeOnlyConverter : JsonConverterFactory
    {
        public static readonly TimeOnlyConverter Instance = new();

        private TimeOnlyConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert == typeof(TimeOnly) || typeToConvert == typeof(TimeOnly?);
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            if (typeToConvert == typeof(TimeOnly))
                return TimeOnlyConverterInner.Instance;

            return NullableTimeOnlyConverter.Instance;
        }

        private sealed class TimeOnlyConverterInner : JsonConverter<TimeOnly>
        {
            public static readonly TimeOnlyConverterInner Instance = new();

            public override unsafe TimeOnly Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.String)
                    throw new InvalidOperationException("Expected string, Got " + reader.TokenType);

                string value = reader.GetString();

                fixed (char* buffer = value.AsSpan())
                {
                    if (LazyStringParser.TryParseTimeOnly(buffer, value.Length, out TimeOnly timeOnly) == false)
                        throw new InvalidOperationException("Expected TimeOnly, Got " + value);

                    return timeOnly;
                }
            }

            public override void Write(Utf8JsonWriter writer, TimeOnly value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(value.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture));
            }
        }

        private sealed class NullableTimeOnlyConverter : JsonConverter<TimeOnly?>
        {
            public static readonly NullableTimeOnlyConverter Instance = new();

            public override unsafe TimeOnly? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                if (reader.TokenType != JsonTokenType.String)
                    throw new InvalidOperationException("Expected string, Got " + reader.TokenType);

                string value = reader.GetString();

                fixed (char* buffer = value.AsSpan())
                {
                    if (LazyStringParser.TryParseTimeOnly(buffer, value.Length, out TimeOnly timeOnly) == false)
                        throw new InvalidOperationException("Expected TimeOnly, Got " + value);

                    return timeOnly;
                }
            }

            public override void Write(Utf8JsonWriter writer, TimeOnly? value, JsonSerializerOptions options)
            {
                if (value == null)
                {
                    writer.WriteNullValue();
                    return;
                }

                writer.WriteStringValue(value.Value.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture));
            }
        }
    }
}
#endif
