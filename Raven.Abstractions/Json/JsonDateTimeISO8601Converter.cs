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
			writer.WriteValue(((DateTime)value).ToString("o", CultureInfo.InvariantCulture));
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			var s = reader.Value as string;
			DateTime time;
			if(s != null && DateTime.TryParseExact(s, "o", CultureInfo.InvariantCulture,DateTimeStyles.RoundtripKind, out time))
			{
				return time;
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
			return typeof (DateTime) == objectType;
		}
	}
}