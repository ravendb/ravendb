using System.Runtime.Serialization;
using Newtonsoft.Json;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Json.Converters
{
    internal sealed class LazyStringValueJsonConverter : RavenTypeJsonConverter<LazyStringValue>
    {
        public static readonly LazyStringValueJsonConverter Instance = new LazyStringValueJsonConverter();

        private LazyStringValueJsonConverter() {}

        protected override void WriteJson(BlittableJsonWriter writer, LazyStringValue value, JsonSerializer serializer)
        {
            writer.WriteValue((object)value);
        }

        internal override LazyStringValue ReadJson(BlittableJsonReader reader)
        {
            //Todo It will be better to change the reader to set the value as LazyStringValue 
            if (!(reader.Value is string strValue))
            {
                throw new SerializationException($"Try to read {nameof(LazyStringValue)} from {reader.Value?.GetType()}. Should be string here");
            }

            return reader.Context.GetLazyString(strValue);
        }
    }
}
