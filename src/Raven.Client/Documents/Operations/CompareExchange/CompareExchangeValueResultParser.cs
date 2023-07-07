using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    internal static class CompareExchangeValueBlittableJsonConverter
    {
        public static object ConvertToBlittable(object value, DocumentConventions conventions, JsonOperationContext context)
        {
            return ConvertToBlittable(value, conventions, context, conventions.Serialization.CreateSerializer());
        }

        public static object ConvertToBlittable(object value, DocumentConventions conventions, JsonOperationContext context, IJsonSerializer jsonSerializer)
        {
            if (value == null)
                return null;

            if (value is ValueType ||
                value is string ||
                value is BlittableJsonReaderArray)
                return value;

            if (value is IEnumerable enumerable && !(enumerable is IDictionary))
            {
                return enumerable.Cast<object>()
                    .Select(v => ConvertToBlittable(v, conventions, context, jsonSerializer));
            }

            return conventions.Serialization.DefaultConverter.ToBlittable(value, context);
        }
    }

    internal static class CompareExchangeValueResultParser<T>
    {
        public static Dictionary<string, CompareExchangeValue<T>> GetValues(BlittableJsonReaderObject response, bool materializeMetadata, DocumentConventions conventions)
        {
            var results = new Dictionary<string, CompareExchangeValue<T>>(StringComparer.OrdinalIgnoreCase);

            if (response == null) // 404
                return results;

            if (response.TryGet("Results", out BlittableJsonReaderArray items) == false)
                throw new InvalidDataException("Response is invalid. Results is missing.");

            foreach (BlittableJsonReaderObject item in items)
            {
                if (item == null)
                    throw new InvalidDataException("Response is invalid. Item is null.");

                var value = GetSingleValue(item, materializeMetadata, conventions);
                results[value.Key] = value;
            }

            return results;
        }

        public static CompareExchangeValue<T> GetValue(BlittableJsonReaderObject response, bool materializeMetadata, DocumentConventions conventions)
        {
            if (response == null)
                return null;

            var value = GetValues(response, materializeMetadata, conventions).FirstOrDefault();
            return value.Value;
        }

        public static CompareExchangeValue<T> GetSingleValue(BlittableJsonReaderObject item, bool materializeMetadata, DocumentConventions conventions)
        {
            if (item == null)
                return null;

            if (item.TryGet(nameof(CompareExchangeValue<T>.Key), out string key) == false)
                throw new InvalidDataException("Response is invalid. Key is missing.");
            if (item.TryGet(nameof(CompareExchangeValue<T>.Index), out long index) == false)
                throw new InvalidDataException("Response is invalid. Index is missing.");
            if (item.TryGet(nameof(CompareExchangeValue<T>.Value), out BlittableJsonReaderObject raw) == false)
                throw new InvalidDataException("Response is invalid. Value is missing.");

            if (raw == null)
                return new CompareExchangeValue<T>(key, index, default);

            MetadataAsDictionary metadata = null;
            if (raw.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject bjro) && bjro != null)
            {
                metadata = materializeMetadata == false
                    ? new MetadataAsDictionary(bjro)
                    : MetadataAsDictionary.MaterializeFromBlittable(bjro);
            }

            var value = DeserializeObject(raw, conventions);
            return new CompareExchangeValue<T>(key, index, value, metadata);
        }

        internal static T DeserializeObject(BlittableJsonReaderObject raw, DocumentConventions conventions)
        {
            if (raw == null)
                return default;

            var type = typeof(T);

            if (type.IsPrimitive || type == typeof(string))
            {
                // simple
                raw.TryGet(Constants.CompareExchange.ObjectFieldName, out T value);
                return value;
            }

            if (type == typeof(BlittableJsonReaderObject))
            {
                if (raw.TryGetMember(Constants.CompareExchange.ObjectFieldName, out object rawValue) == false)
                {
                    return default;
                }

                switch (rawValue)
                {
                    case null:
                        return default;
                    case BlittableJsonReaderObject _:
                        return (T)rawValue;
                    default:
                        return (T)(object)raw;
                }
            }

            if (raw.TryGetMember(Constants.CompareExchange.ObjectFieldName, out _) == false)
            {
                return conventions.Serialization.DefaultConverter.FromBlittable<T>(raw);;
            }

            var converted = conventions.Serialization.DefaultConverter.FromBlittable<ResultHolder>(raw);
            return converted.Object;
        }

        private class ResultHolder
        {
#pragma warning disable 649
            public T Object;
#pragma warning restore 649
        }
    }
}
