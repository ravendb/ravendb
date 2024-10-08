using System.IO;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    /// <summary>
    /// Represents the result of a delete\put compare-exchange operation, containing the value, index, and success status.
    /// </summary>
    /// <typeparam name="T">The type of the value stored in the compare-exchange result.</typeparam>
    public sealed class CompareExchangeResult<T>
    {
        /// <summary>
        /// The value associated with the compare-exchange operation.
        /// </summary>
        public T Value;

        /// <summary>
        /// The index of the compare-exchange.
        /// </summary>
        public long Index;

        /// <summary>
        /// Indicates whether the compare-exchange operation was successful.
        /// </summary>
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
