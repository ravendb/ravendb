#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
using System;
using System.Globalization;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal sealed class DateOnlyConverter : JsonConverter
    {
        // ISO 8601 standard
        // More info available at: https://docs.microsoft.com/en-us/dotnet/standard/base-types/standard-date-and-time-format-strings#Roundtrip

        public static readonly DateOnlyConverter Instance = new();

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            var @do = (DateOnly)value;
            writer.WriteValue(@do.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture));
        }

        public override unsafe object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            if (reader.TokenType == JsonToken.PropertyName)
                return reader.ReadAsString();

            if (reader.TokenType != JsonToken.String)
                throw new InvalidOperationException("Expected string, Got " + reader.TokenType);

            string value = (string)reader.Value;

            fixed (char* buffer = value.AsSpan())
            {
                if (LazyStringParser.TryParseDateOnly(buffer, value.Length, out var dateOnly) == false)
                {
                    if (LazyStringParser.TryParseDateTime(buffer, value.Length, out var dt, out var _, true) is LazyStringParser.Result.Failed
                        or LazyStringParser.Result.DateTimeOffset)
                    {
                        throw new InvalidOperationException("Expected DateOnly, Got " + value);
                    }

                    return DateOnly.FromDateTime(dt);
                }

                return dateOnly;
            }
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(DateOnly) || objectType == typeof(DateOnly?);
        }
    }
}

#endif
