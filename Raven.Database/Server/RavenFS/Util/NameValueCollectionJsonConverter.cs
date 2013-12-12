using System;
using System.Collections.Specialized;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.RavenFS.Util
{
	public class NameValueCollectionJsonConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			var collection = (NameValueCollection)value;

			writer.WriteStartObject();

			foreach (var key in collection.AllKeys)
			{
				writer.WritePropertyName(key);

				var values = collection.GetValues(key);
				if (values == null)
				{
					writer.WriteNull();
					continue;
				}
				if (values.Length == 1)
				{
					writer.WriteValue(values[0]);
				}
				else
				{
					writer.WriteStartArray();

					foreach (var item in values)
					{
						writer.WriteValue(item);
					}

					writer.WriteEndArray();
				}

			}
			writer.WriteEndObject();
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			var collection = new NameValueCollection();

			while (reader.Read())
			{
				if (reader.TokenType == JsonToken.EndObject)
					break;

				var key = (string)reader.Value;

				if (reader.Read() == false)
					throw new InvalidOperationException("Expected PropertyName, got " + reader.TokenType);

				if (reader.TokenType == JsonToken.StartArray)
				{
					var values = serializer.Deserialize<string[]>(reader);
					foreach (var value in values)
					{
						collection.Add(key, value);
					}
				}
				else
				{
					collection.Add(key, reader.Value.ToString());
				}
			}

			return collection;
		}

		public override bool CanConvert(Type objectType)
		{
			return
				objectType == typeof(NameValueCollection) ||
				objectType.IsSubclassOf(typeof(NameValueCollection));
		}
	}
}