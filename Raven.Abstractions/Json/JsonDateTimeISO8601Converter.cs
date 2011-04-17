using System;
using System.Globalization;
using Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
	public class JsonDateTimeISO8601Converter : RavenJsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			if(value is DateTime)
			{
				var dateTime = ((DateTime)value);
				if (dateTime.Kind == DateTimeKind.Unspecified)
					dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
				writer.WriteValue(dateTime.ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture));
			}
			else if (value is DateTimeOffset)
				writer.WriteValue(((DateTimeOffset) value).ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture));
			else
				throw new ArgumentException("Not idea how to process argument: " + value);
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			var s = reader.Value as string;
		    if(s != null)
			{
				if (objectType == typeof(DateTime) || objectType == typeof(DateTime?))
				{
					DateTime time;
					if (DateTime.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
					                           DateTimeStyles.RoundtripKind, out time))
					{
						if (time.Kind == DateTimeKind.Unspecified)
							return DateTime.SpecifyKind(time, DateTimeKind.Local);
						return time;
					}
				}
				if(objectType == typeof(DateTimeOffset) || objectType == typeof(DateTimeOffset?))
				{
					DateTimeOffset time;
					if (DateTimeOffset.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
											   DateTimeStyles.RoundtripKind, out time))
						return time;
				}

			}
			return DeferReadToNextConverter(reader, objectType, serializer, existingValue);
		}

		public override bool CanConvert(Type objectType)
		{
			return typeof (DateTime) == objectType ||
				typeof(DateTimeOffset) == objectType ||
				typeof(DateTimeOffset?) == objectType ||
				typeof(DateTime?) == objectType;
		}
	}
}