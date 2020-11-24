using System;
using Newtonsoft.Json;
using Sparrow;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal sealed class SizeConverter : JsonConverter
    {
        public static readonly SizeConverter Instance = new SizeConverter();

        private SizeConverter()
        {
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            var valueAsSize = (Size)value;
            writer.WriteValue(valueAsSize.GetValue(SizeUnit.Bytes));
        }

        public override unsafe object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            if (reader.TokenType != JsonToken.Integer)
                throw new InvalidOperationException("Expected Integer, Got " + reader.TokenType);

            return new Size(reader.ReadAsInt32().Value, SizeUnit.Bytes);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Size) || objectType == typeof(Size?);
        }
    }
}
