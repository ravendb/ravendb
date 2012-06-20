using System;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Json
{
	public class JsonToJsonConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			if (value is RavenJToken)
				((RavenJToken)value).WriteTo(writer);
#if !NET35
			else if(value is DynamicNullObject)
				writer.WriteNull();
			else
				((IDynamicJsonObject)value).Inner.WriteTo(writer);
#else
			throw new NotImplementedException();
#endif
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			// NOTE: THIS DOESN'T SUPPORT READING OF DynamicJsonObject !!!

			var o = RavenJToken.Load(reader);
			return (o.Type == JTokenType.Null || o.Type == JTokenType.Undefined) ? null : o;
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof(RavenJToken) || objectType.IsSubclassOf(typeof(RavenJToken))
#if !NET35
				|| objectType == typeof(DynamicJsonObject) || objectType == typeof(DynamicNullObject)
#endif
				;
		}
	}
}