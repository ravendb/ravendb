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
            if (response.TryGet("Results", out BlittableJsonReaderArray items) == false)
                    throw new InvalidDataException("Response is invalid. Results is missing.");

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
                    T value = default(T);
                    raw?.TryGet("Object", out value);
                    results[key] = new CompareExchangeValue<T>(key, index, value);
                }
                else
                {
                    BlittableJsonReaderObject val = null;
                    raw?.TryGet("Object", out val);
                    if (val == null)
                    {
                        results[key] = new CompareExchangeValue<T>(key, index, default(T));
                    }
                    else
                    {
                        var convereted = EntityToBlittable.ConvertToEntity(typeof(T), null, val, conventions);
                        results[key] = new CompareExchangeValue<T>(key, index, (T)convereted);
                    }
                }
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
    }
}
