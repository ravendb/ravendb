using System;
using Newtonsoft.Json;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal sealed class LazyStringValueJsonConverter : RavenJsonConverter
    {
        //Todo To consider put context as part of the converter and not part of the reader
        //public BlittableJsonConverter(JsonOperationContext context)
        //{
        //    _context = context;
        //}


        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            //Todo To consider put context as part of the converter and not part of the reader
            //if (reader.Value is LazyStringValue lazyString)
            //{
            //    return lazyString.Clone(_context);
            //}
            //throw new Exception($"Value must be {nameof(BlittableJsonReaderObject)}");

            if (reader is BlittableJsonReader blittableReader)
            {
                return blittableReader.ReadAsLazyStringValue();
            }
            throw new Exception($"{nameof(BlittableJsonReader)} must to be used for convert to {nameof(BlittableJsonReaderObject)}");
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(LazyStringValue);
        }
    }
}
