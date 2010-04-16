using System;
using Newtonsoft.Json;

namespace Raven.Database.Json
{
	public class JsonEnumConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value)
		{
			writer.WriteValue(value.ToString());
		}

		public override object ReadJson(JsonReader reader, Type objectType)
		{
			return Enum.Parse(objectType, (string)reader.Value);
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType.IsEnum;
		}
	}
}