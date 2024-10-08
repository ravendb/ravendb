using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    /// <summary>
    /// Operation to delete a compare exchange value in a RavenDB database. 
    /// A compare exchange value is a key-value pair that is part of the RavenDB distributed coordination mechanism.
    /// </summary>
    /// <typeparam name="T">The type of the value associated with the compare exchange key.</typeparam>
    public sealed class DeleteCompareExchangeValueOperation<T> : IOperation<CompareExchangeResult<T>>
    {
        private readonly string _key;
        private readonly long _index;

        /// <summary>
        /// Operation to delete a compare exchange value in a RavenDB database. 
        /// A compare exchange value is a key-value pair that is part of the RavenDB distributed coordination mechanism.
        /// Initializes a new instance of the <see cref="DeleteCompareExchangeValueOperation{T}"/> class.
        /// </summary>
        /// <typeparam name="T">The type of the value associated with the compare exchange key.</typeparam>
        /// <param name="key">The key of the compare exchange value to delete.</param>
        /// <param name="index">The index of the compare exchange value to delete.</param>
        public DeleteCompareExchangeValueOperation(string key, long index)
        {
            _key = key;
            _index = index;
        }

        public RavenCommand<CompareExchangeResult<T>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteCompareExchangeValueCommand(_key, _index, conventions);
        }

        private sealed class DeleteCompareExchangeValueCommand : RavenCommand<CompareExchangeResult<T>>, IRaftCommand
        {
            private readonly string _key;
            private readonly long _index;
            private readonly DocumentConventions _conventions;

            public DeleteCompareExchangeValueCommand(string key, long index, DocumentConventions conventions = null)
            {
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentNullException(nameof(key), "The key argument must have value");

                _key = key;
                _index = index;
                _conventions = conventions ?? DocumentConventions.Default;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/cmpxchg?key={Uri.EscapeDataString(_key)}&index={_index}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Delete,
                };
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = CompareExchangeResult<T>.ParseFromBlittable(response, _conventions);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

    }
}
