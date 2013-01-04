using System;
using System.Globalization;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
	public class JsonFloatConverter : JsonConverter
	{
		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
		{
			if(value == null)
			{
				writer.WriteNull();
				return;
			}
			var f = (float)value;
			writer.WriteValue(EnsureDecimalPlace(f, f.ToString("R", CultureInfo.InvariantCulture)));
		}


		private static string EnsureDecimalPlace(float value, string text)
		{
			if ((!float.IsNaN(value) && !float.IsInfinity(value)) && ((text.IndexOf('.') == -1) && (text.IndexOf('E') == -1)))
			{
				return (text + ".0");
			}
			return text;
		}


		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
		{
			string value = reader.Value as string;
			if (value != null)
			{
				float result;
				if (float.TryParse(value, NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign,
				                   CultureInfo.InvariantCulture, out result))
					return result;
			}
			if (reader.Value == null)
				return null;
			return Convert.ChangeType(reader.Value, typeof(float), CultureInfo.InvariantCulture);
	  
		}

		public override bool CanConvert(Type objectType)
		{
			return objectType == typeof (float) || objectType == typeof(float?);
		}
	}
}