//-----------------------------------------------------------------------
// <copyright file="JsonEnumConverter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
	/// <summary>
	/// Convert an enum to a json string
	/// </summary>
	public class JsonEnumConverter : JsonConverter
	{
		/// <summary>
		/// Writes the JSON representation of the object.
		/// </summary>
		/// <param name="writer">The <see cref="T:Raven.Imports.Newtonsoft.Json.JsonWriter"/> to write to.</param>
		/// <param name="value">The value.</param>
		/// <param name="serializer">The calling serializer.</param>
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			if(value == null)
				writer.WriteNull();
			else
				writer.WriteValue(value.ToString());
		}

		/// <summary>
		/// Reads the JSON representation of the object.
		/// </summary>
		/// <param name="reader">The <see cref="T:Raven.Imports.Newtonsoft.Json.JsonReader"/> to read from.</param>
		/// <param name="objectType">Type of the object.</param>
		/// <param name="existingValue">The existing value of object being read.</param>
		/// <param name="serializer">The calling serializer.</param>
		/// <returns>The object value.</returns>
		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			var enumType = Nullable.GetUnderlyingType(objectType) ?? objectType;
			if (enumType != objectType && reader.TokenType == JsonToken.Null)//nullable
				return null;
			return Enum.Parse(enumType, reader.Value.ToString(), true);
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
			return (Nullable.GetUnderlyingType(objectType) ?? objectType).IsEnum;
		}
	}
}
