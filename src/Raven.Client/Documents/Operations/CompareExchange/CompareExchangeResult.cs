using System.IO;
using Raven.Client.Documents.Conventions;
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

            var result = CompareExchangeValueResultParser<T>.DeserializeObject(raw, conventions);

            return new CompareExchangeResult<T>
            {
                Index = index,
                Value = result,
                Successful = successful
            };
        }
    }
}
