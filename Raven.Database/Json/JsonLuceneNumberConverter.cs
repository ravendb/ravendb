using System;
using System.Globalization;
using Newtonsoft.Json;

namespace Raven.Database.Json
{
	public class JsonLuceneNumberConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value)
		{
			if (value is long)
				writer.WriteValue(NumberToString((long)value));
			else if( value is int)
				writer.WriteValue(NumberToString((int)value));
			else
				throw new NotSupportedException("Only long & int are supported");
		}

		public override object ReadJson(JsonReader reader, Type objectType)
		{
			if (reader.Value is long || reader.Value is int)
				return reader.Value;
			return ParseNumber((string) reader.Value);
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof(long) || objectType == typeof(int);
		}

		public static string NumberToString(int number)
		{
			return string.Format("0x{0:X8}", number);
		}

		public static string NumberToString(long number)
		{
			return string.Format("0x{0:X16}", number);
		}

		public static object ParseNumber(string number)
		{
			return number.Length == 16 ? 
				long.Parse(number, NumberStyles.AllowHexSpecifier | NumberStyles.HexNumber) : 
				int.Parse(number, NumberStyles.AllowHexSpecifier | NumberStyles.HexNumber);
		}
	}
}