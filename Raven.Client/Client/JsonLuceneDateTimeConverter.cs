using System;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Client.Linq;

namespace Raven.Database.Json
{
	public class JsonLuceneDateTimeConverter : JsonConverter
	{
		// 17 numeric characters on a datetime field == Lucene datetime
		private static readonly Regex luceneDateTimePattern = new Regex(@"\d{17}",RegexOptions.Compiled);

		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			throw new NotImplementedException();
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