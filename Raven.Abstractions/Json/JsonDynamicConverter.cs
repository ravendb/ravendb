using System;
using System.Dynamic;
using System.Linq.Expressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Linq;

namespace Raven.Database.Json
{
	/// <summary>
	/// Convert a dynamic variable to a json value and vice versa
	/// </summary>
	public class JsonDynamicConverter : JsonConverter
	{
		/// <summary>
		/// Writes the JSON representation of the object.
		/// </summary>
		/// <param name="writer">The <see cref="T:Newtonsoft.Json.JsonWriter"/> to write to.</param>
		/// <param name="value">The value.</param>
		/// <param name="serializer">The calling serializer.</param>
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

		/// <summary>
		/// Reads the JSON representation of the object.
		/// </summary>
		/// <param name="reader">The <see cref="T:Newtonsoft.Json.JsonReader"/> to read from.</param>
		/// <param name="objectType">Type of the object.</param>
		/// <param name="existingValue">The existing value of object being read.</param>
		/// <param name="serializer">The calling serializer.</param>
		/// <returns>The object value.</returns>
		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			return new DynamicJsonObject((JObject)JToken.ReadFrom(reader));
		}

		/// <summary>
		/// Determines whether this instance can convert the specified object type.
		/// </summary>
		/// <param name="objectType">Type of the object.</param>
		/// <returns>
		/// 	<c>true</c> if this instance can convert the specified object type; otherwise, <c>false</c>.
		/// </returns>
		public override bool CanConvert(Type objectType)
		{
			return typeof(IDynamicMetaObjectProvider).IsAssignableFrom(objectType);
		}
	}
}
