using System;
using System.Globalization;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal sealed class JsonDateTimeISO8601Converter : JsonConverter
    {
        public static readonly JsonDateTimeISO8601Converter Instance = new JsonDateTimeISO8601Converter();
        
        private JsonDateTimeISO8601Converter()
        {
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            switch (value)
            {
                case DateTime dateTime:
                {
                    if (dateTime.Kind == DateTimeKind.Unspecified)
                        dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
                    writer.WriteValue(dateTime.GetDefaultRavenFormat());
                    break;
                }
                case DateTimeOffset dateTimeOffset:
                    writer.WriteValue(dateTimeOffset.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
                    break;
                default:
                    throw new ArgumentException($"Not idea how to process argument: '{value}'");
            }
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.Value is string s)
            {
                if (objectType == typeof(DateTime) || objectType == typeof(DateTime?))
                {
                    if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime time))
                    {
                        if (s.EndsWith("+00:00"))
                            return time.ToUniversalTime();
                        return time;
                    }
                }
                if (objectType == typeof(DateTimeOffset) || objectType == typeof(DateTimeOffset?))
                {
                    if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset time))
                        return time;
                }
            }
            return JsonLuceneDateTimeConverter.Instance.ReadJson(reader, objectType, existingValue,serializer);
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(DateTime) == objectType ||
                typeof(DateTimeOffset) == objectType ||
                typeof(DateTimeOffset?) == objectType ||
                typeof(DateTime?) == objectType;
        }
    }
}
