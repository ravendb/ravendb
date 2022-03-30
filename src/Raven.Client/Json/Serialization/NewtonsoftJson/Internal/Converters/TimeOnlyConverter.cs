using System;
using System.Globalization;
using System.IO;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;

#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
// ISO 8601 standard
// More info available at: https://docs.microsoft.com/en-us/dotnet/standard/base-types/standard-date-and-time-format-strings#Roundtrip
internal sealed class TimeOnlyConverter : JsonConverter
{
    public static readonly TimeOnlyConverter Instance = new();

    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        if (value == null)
        {
            writer.WriteNull();
            return;
        }

        var to = (TimeOnly)value;
        writer.WriteValue(to.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture));
    }

    public override unsafe object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
            return null;

        if (reader.TokenType != JsonToken.String)
            throw new InvalidOperationException("Expected string, Got " + reader.TokenType);

        string value = (string)reader.Value;

        fixed (char* buffer = value.AsSpan())
        {
            if (LazyStringParser.TryParseTimeOnly(buffer, value.Length, out var timeOnly) == false)
            {
                throw new InvalidOperationException("Expected TimeOnly , Got " + value);
            }

            return timeOnly;
        }
    }

    public override bool CanConvert(Type objectType)
    {
        return objectType == typeof(TimeOnly) || objectType == typeof(TimeOnly?);
    }
}
#endif
