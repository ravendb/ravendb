using System;
using Newtonsoft.Json;

namespace Raven.Database.Json
{
	public class JsonEnumConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			writer.WriteValue(value.ToString());
		}

	    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
	    {
			return Enum.Parse(objectType, (string)reader.Value);
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType.IsEnum;
		}
	}
}