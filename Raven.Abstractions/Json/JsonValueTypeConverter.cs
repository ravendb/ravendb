using System;
using System.Globalization;
using Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
    public class JsonValueTypeConverter<T> : JsonConverter
    {
        public delegate bool TryParse(string s, NumberStyles styles, IFormatProvider provider, out T val);

        private TryParse tryParse;

        public JsonValueTypeConverter(TryParse tryParse)
        {
            this.tryParse = tryParse;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var s = reader.Value as string;
            T val;
            if (s != null && tryParse(s, NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out val))
                return val;
        	if (reader.Value == null)
        		return null;
        	return Convert.ChangeType(reader.Value, typeof(T), CultureInfo.InvariantCulture);
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof (T) == objectType;
        }

        public override bool CanWrite
        {
            get { return false; }
        }
    }
}