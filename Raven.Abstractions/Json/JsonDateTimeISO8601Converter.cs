using System;
using System.Globalization;
using Newtonsoft.Json;
using System.Linq;

namespace Raven.Abstractions.Json
{
	public class JsonDateTimeISO8601Converter : JsonConverter
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
				if (objectType == typeof(DateTime))
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
				if(objectType == typeof(DateTimeOffset))
				{
					DateTimeOffset time;
					if (DateTimeOffset.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
											   DateTimeStyles.RoundtripKind, out time))
						return time;
				}

			}
			var anotherConverter =
				serializer.Converters.Skip(serializer.Converters.IndexOf(this)+1)
				.Where(x => x.CanConvert(objectType))
				.FirstOrDefault();
			if (anotherConverter != null)
				return anotherConverter.ReadJson(reader, objectType, existingValue, serializer);
			return reader.Value;
		}

		public override bool CanConvert(Type objectType)
		{
			return typeof (DateTime) == objectType || typeof (DateTimeOffset) == objectType;
		}
	}
}