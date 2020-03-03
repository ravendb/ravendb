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
        public static Dictionary<string, CompareExchangeValue<T>> GetValues(JsonOperationContext context, BlittableJsonReaderObject response, DocumentConventions conventions)
        {
            if (response.TryGet("Results", out BlittableJsonReaderArray items) == false)
                throw new InvalidDataException("Response is invalid. Results is missing.");

            LazyStringValue lsv = null;
            var results = new Dictionary<string, CompareExchangeValue<T>>();

            foreach (BlittableJsonReaderObject item in items)
            {
                if (item == null)
                    throw new InvalidDataException("Response is invalid. Item is null.");

                if (item.TryGet("Key", out string key) == false)
                    throw new InvalidDataException("Response is invalid. Key is missing.");
                if (item.TryGet("Index", out long index) == false)
                    throw new InvalidDataException("Response is invalid. Index is missing.");
                if (item.TryGet("Value", out BlittableJsonReaderObject raw) == false)
                    throw new InvalidDataException("Response is invalid. Value is missing.");

                if (typeof(T).GetTypeInfo().IsPrimitive || typeof(T) == typeof(string))
                {
                    // simple
                    T value = default;
                    raw?.TryGet(Constants.CompareExchange.ObjectFieldName, out value);
                    results[key] = new CompareExchangeValue<T>(key, index, value);
                }
                else
                {
                    if (lsv == null)
                        lsv = context.GetLazyString(Constants.CompareExchange.ObjectFieldName);

                    if (raw == null || raw.Contains(lsv) == false)
                    {
                        results[key] = new CompareExchangeValue<T>(key, index, default);
                    }
                    else
                    {
                        var converted = (ResultHolder)EntityToBlittable.ConvertToEntity(typeof(ResultHolder), null, raw, conventions);
                        results[key] = new CompareExchangeValue<T>(key, index, converted.Object);
                    }
                }
            }

            return results;
        }

        public static CompareExchangeValue<T> GetValue(JsonOperationContext context, BlittableJsonReaderObject response, DocumentConventions conventions)
        {
            if (response == null)
                return null;

            var value = GetValues(context, response, conventions).FirstOrDefault();
            return value.Value;
        }

        private class ResultHolder
        {
#pragma warning disable 649
            public T Object;
#pragma warning restore 649
        }
    }
}
