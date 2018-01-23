using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    public class DeleteCompareExchangeValueOperation<T> : IOperation<CompareExchangeResult<T>>
    {
        private readonly string _key;
        private readonly long _index;

        public DeleteCompareExchangeValueOperation(string key, long index)
        {
            _key = key;
            _index = index;
        }

        public RavenCommand<CompareExchangeResult<T>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteCompareExchangeValueCommand(_key, _index, conventions);
        }

        private class DeleteCompareExchangeValueCommand : RavenCommand<CompareExchangeResult<T>>
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
                url = $"{node.Url}/databases/{node.Database}/cmpxchg?key={_key}&index={_index}";
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
        }

    }
}
