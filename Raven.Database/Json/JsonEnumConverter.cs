using System;
using Newtonsoft.Json;

namespace Raven.Database.Json
{
	/// <summary>
	/// Convert an enum to a json string
	/// </summary>
	public class JsonEnumConverter : JsonConverter
	{
		/// <summary>
		/// Writes the JSON representation of the object.
		/// </summary>
		/// <param name="writer">The <see cref="T:Newtonsoft.Json.JsonWriter"/> to write to.</param>
		/// <param name="value">The value.</param>
		/// <param name="serializer">The calling serializer.</param>
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			writer.WriteValue(value.ToString());
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			return Enum.Parse(objectType, reader.Value.ToString());
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType.IsEnum;
		}
	}
}