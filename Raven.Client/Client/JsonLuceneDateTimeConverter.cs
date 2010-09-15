using System;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Client.Linq;

namespace Raven.Database.Json
{
	/// <summary>
	/// Convert a lucene data format to and from json values
	/// </summary>
	public class JsonLuceneDateTimeConverter : JsonConverter
	{
		// 17 numeric characters on a datetime field == Lucene datetime
		private static readonly Regex luceneDateTimePattern = new Regex(@"\d{17}",RegexOptions.Compiled);

		/// <summary>
		/// Writes the JSON representation of the object.
		/// </summary>
		/// <param name="writer">The <see cref="T:Newtonsoft.Json.JsonWriter"/> to write to.</param>
		/// <param name="value">The value.</param>
		/// <param name="serializer">The calling serializer.</param>
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			writer.WriteValue(value);
		}

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			var input = reader.Value as string;
			if (input != null && luceneDateTimePattern.IsMatch(input))
				return DateTools.StringToDate(input);
			return reader.Value;
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof (DateTime);
		}

		public override bool CanWrite
		{
			get
			{
				return false;
			}
		}
	}
}