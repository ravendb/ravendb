using System;
using System.Collections;
using System.Collections.Concurrent;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal class JsonEnumerableConverter : JsonConverter
    {
        private readonly NewtonsoftJsonSerializationConventions _conventions;

        public override bool CanRead => false;

        public JsonEnumerableConverter(NewtonsoftJsonSerializationConventions conventions)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        }

        public override bool CanConvert(Type objectType)
        {
            if (objectType == null)
                return false;

            if (objectType == typeof(LazyStringValue))
                return false;

            if (objectType == typeof(BlittableJsonReaderArray))
                return false;

            var jsonArrayContract = _conventions.JsonContractResolver.ResolveContract(objectType) as JsonArrayContract;
            if (jsonArrayContract == null)
                return false;

            if (jsonArrayContract.IsMultidimensionalArray)
                return false;

            return jsonArrayContract.CollectionItemType != typeof(object);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotSupportedException();
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteStartArray();

            var contract = (JsonArrayContract)serializer.ContractResolver.ResolveContract(value.GetType());

            foreach (object val in (IEnumerable)value)
            {
                if (contract.ItemConverter != null && contract.ItemConverter.CanWrite)
                {
                    contract.ItemConverter.WriteJson(writer, val, serializer);
                    continue;
                }

                serializer.Serialize(writer, val, contract.CollectionItemType);
            }
                

            writer.WriteEndArray();
        }
    }
}
