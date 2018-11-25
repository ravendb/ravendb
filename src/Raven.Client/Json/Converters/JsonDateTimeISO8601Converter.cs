using System;
using System.Globalization;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal sealed class JsonDateTimeISO8601Converter : RavenJsonConverter
    {
        public static readonly JsonDateTimeISO8601Converter Instance = new JsonDateTimeISO8601Converter();

        private JsonDateTimeISO8601Converter()
        {
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value is DateTime)
            {
                var dateTime = (DateTime)value;
                if (dateTime.Kind == DateTimeKind.Unspecified)
                    dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
                writer.WriteValue(dateTime.GetDefaultRavenFormat(dateTime.Kind == DateTimeKind.Utc));
            }
            else if (value is DateTimeOffset)
            {
                var dateTimeOffset = (DateTimeOffset)value;
                writer.WriteValue(dateTimeOffset.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
            }
            else
                throw new ArgumentException(string.Format("Not idea how to process argument: '{0}'", value));
        }

        public override unsafe object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.Value is string s)
            {
                fixed (char * str = s)
                {
                    var result = LazyStringParser.TryParseDateTime(str, s.Length, out DateTime dt, out DateTimeOffset dto);
                    if (result == LazyStringParser.Result.DateTime)
                        return dt;
                    if (result == LazyStringParser.Result.DateTimeOffset)
                        return dto;
                }
            }

            return DeferReadToNextConverter(reader, objectType, serializer, existingValue);
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
