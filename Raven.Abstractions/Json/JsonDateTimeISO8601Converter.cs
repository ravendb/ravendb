using System;
using System.Globalization;
using Raven.Imports.Newtonsoft.Json;

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
				var postFix = dateTime.Kind == DateTimeKind.Utc ? "Z" : "";
				writer.WriteValue(dateTime.ToString(Default.DateTimeFormatsToWrite + postFix, CultureInfo.InvariantCulture));
			}
			else if (value is DateTimeOffset)
			{
				var dateTimeOffset = ((DateTimeOffset) value);
				if(dateTimeOffset.Offset==TimeSpan.Zero)
				{
					writer.WriteValue(dateTimeOffset.UtcDateTime.ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture) + "Z");
				}
				else
				{
					writer.WriteValue(dateTimeOffset.ToString(Default.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
				}
			}
			else
				throw new ArgumentException(string.Format("Not idea how to process argument: '{0}'", value));
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
						if (s.EndsWith("+00:00"))
							return time.ToUniversalTime();
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