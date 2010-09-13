using System;
using System.Dynamic;
using System.Linq.Expressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Linq;

namespace Raven.Database.Json
{
	public class JsonDynamicConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			var dynamicValue = ((IDynamicMetaObjectProvider) value).GetMetaObject(Expression.Constant(value));

			writer.WriteStartObject();
			foreach (var dynamicMemberName in dynamicValue.GetDynamicMemberNames())
			{
				writer.WritePropertyName(dynamicMemberName);
				var memberValue = DynamicUtil.GetValueDynamically(value, dynamicMemberName);
				if(memberValue == null || memberValue is ValueType || memberValue is string)
					writer.WriteValue(memberValue);
				else
					serializer.Serialize(writer, memberValue);
			}
			writer.WriteEndObject();

		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			return new DynamicJsonObject((JObject)JToken.ReadFrom(reader));
		}

		public override bool CanConvert(Type objectType)
		{
			return typeof(IDynamicMetaObjectProvider).IsAssignableFrom(objectType);
		}
	}
}