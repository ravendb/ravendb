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
    internal sealed class DateOnlyConverter : JsonConverter<DateOnly>
    {
        public static readonly DateOnlyConverter Instance = new();

        private DateOnlyConverter()
        {
        }

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
}
#endif
