using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    internal static class CompareExchangeValueResultParser<T>
    {
        public static Dictionary<string, CompareExchangeValue<T>> GetValues(BlittableJsonReaderObject response, DocumentConventions conventions)
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

                var value = GetSingleValue(item, conventions);
                results[value.Key] = value;
            }

            return results;
        }

        public static CompareExchangeValue<T> GetValue(BlittableJsonReaderObject response, DocumentConventions conventions)
        {
            if (response == null)
                return null;

            var value = GetValues(response, conventions).FirstOrDefault();
            return value.Value;
        }

        public static CompareExchangeValue<T> GetSingleValue(BlittableJsonReaderObject item, DocumentConventions conventions)
        {
            if (item == null)
                return null;

            if (item.TryGet(nameof(CompareExchangeValue<T>.Key), out string key) == false)
                throw new InvalidDataException("Response is invalid. Key is missing.");
            if (item.TryGet(nameof(CompareExchangeValue<T>.Index), out long index) == false)
                throw new InvalidDataException("Response is invalid. Index is missing.");
            if (item.TryGet(nameof(CompareExchangeValue<T>.Value), out BlittableJsonReaderObject raw) == false)
                throw new InvalidDataException("Response is invalid. Value is missing.");

            var type = typeof(T);

            if (type.GetTypeInfo().IsPrimitive || type == typeof(string))
            {
                // simple
                T value = default;
                raw?.TryGet(Constants.CompareExchange.ObjectFieldName, out value);
                return new CompareExchangeValue<T>(key, index, value);
            }
            else if (type == typeof(BlittableJsonReaderObject))
            {
                if (raw == null || raw.TryGetMember(Constants.CompareExchange.ObjectFieldName, out object rawValue) == false)
                    return new CompareExchangeValue<T>(key, index, default);

                if (rawValue is null)
                    return new CompareExchangeValue<T>(key, index, default);
                else if (rawValue is BlittableJsonReaderObject)
                    return new CompareExchangeValue<T>(key, index, (T)rawValue);
                else
                    return new CompareExchangeValue<T>(key, index, (T)(object)raw);
            }
            else
            {
                if (raw == null || raw.TryGetMember(Constants.CompareExchange.ObjectFieldName, out _) == false)
                {
                    return new CompareExchangeValue<T>(key, index, default);
                }
                else
                {
                    var converted = (ResultHolder)EntityToBlittable.ConvertToEntity(typeof(ResultHolder), null, raw, conventions);
                    return new CompareExchangeValue<T>(key, index, converted.Object);
                }
            }
        }

        private class ResultHolder
        {
#pragma warning disable 649
            public T Object;
#pragma warning restore 649
        }
    }
}
