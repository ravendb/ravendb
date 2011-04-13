#if !NET_3_5
using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Json
{
	public class JsonToJsonConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			if(value is DynamicNullObject)
				writer.WriteNull();
			else if (value is RavenJObject)
				((RavenJObject)value).WriteTo(writer);
			else
				((DynamicJsonObject)value).Inner.WriteTo(writer);
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			// NOTE: THIS DOESN'T SUPPORT READING OF DynamicJsonObject !!!

			var o = RavenJToken.Load(reader);
			return (o.Type == JTokenType.Null || o.Type == JTokenType.Undefined) ? null : o;
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof(RavenJObject) || objectType == typeof(DynamicJsonObject) || objectType == typeof(DynamicNullObject);
		}
	}
}
#endif
