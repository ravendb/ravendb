using System;
using System.Collections;
using System.Collections.Concurrent;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal class JsonEnumerableConverter : RavenJsonConverter
    {
        private readonly ConcurrentDictionary<Type, bool> _cache = new ConcurrentDictionary<Type, bool>();

        private readonly DocumentConventions _conventions;

        public override bool CanRead => false;

        public JsonEnumerableConverter(DocumentConventions conventions)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        }

        public override bool CanConvert(Type objectType)
        {
            if (objectType == null)
                return false;

            return _cache.GetOrAdd(objectType, CanConvertInternal);
        }

        private bool CanConvertInternal(Type objectType)
        {
            if (objectType == typeof(LazyStringValue))
                return false;

            if (objectType == typeof(BlittableJsonReaderArray))
                return false;

            return _conventions.JsonContractResolver.ResolveContract(objectType) is JsonArrayContract;
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
                serializer.Serialize(writer, val, contract.CollectionItemType);

            writer.WriteEndArray();
        }
    }
}
