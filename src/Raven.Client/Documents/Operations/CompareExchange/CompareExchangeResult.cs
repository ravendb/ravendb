using System.IO;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    public class CompareExchangeResult<T>
    {
        public T Value;
        public long Index;
        public bool Successful;

        public static CompareExchangeResult<T> ParseFromBlittable(BlittableJsonReaderObject response, DocumentConventions conventions)
        {
            if (response.TryGet(nameof(Index), out long index) == false)
                throw new InvalidDataException("Response is invalid. Index is missing.");

            response.TryGet(nameof(Successful), out bool successful);
            response.TryGet(nameof(Value), out BlittableJsonReaderObject raw);

            T result;
            object val = null;
            raw?.TryGet(Constants.CompareExchange.ObjectFieldName, out val);

            if (val == null)
            {
                return new CompareExchangeResult<T>
                {
                    Index = index,
                    Value = default(T),
                    Successful = successful
                };
            }
            if (val is BlittableJsonReaderObject obj)
            {
                result = conventions.Serialization.DefaultConverter.FromBlittable<T>(obj, "cluster-value");
            }
            else
            {
                raw.TryGet(Constants.CompareExchange.ObjectFieldName, out result);
            }

            return new CompareExchangeResult<T>
            {
                Index = index,
                Value = result,
                Successful = successful
            };
        }
    }
}
