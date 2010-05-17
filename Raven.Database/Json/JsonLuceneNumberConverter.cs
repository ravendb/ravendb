using System;
using System.Globalization;
using Newtonsoft.Json;

namespace Raven.Database.Json
{
	public class JsonLuceneNumberConverter : JsonConverter
	{
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
			if (value is long)
				writer.WriteValue(NumberToString((long)value));
			else if( value is int)
				writer.WriteValue(NumberToString((int)value));
			else
				throw new NotSupportedException("Only long & int are supported");
		}

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
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

        public static string NumberToString(decimal number)
        {
            return number.ToString(CultureInfo.InvariantCulture);
        }

        public static string NumberToString(float number)
        {
            return number.ToString(CultureInfo.InvariantCulture);
        }

        public static string NumberToString(double number)
        {
            return number.ToString(CultureInfo.InvariantCulture);
        }

		public static object ParseNumber(string number)
		{
			return number.Length == 18 ? 
				(object)Convert.ToInt64(number, 16) :
				Convert.ToInt32(number, 16);
		}
	}
}