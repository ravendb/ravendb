using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    public class GetCompareExchangeIndexOperation : IOperation<Dictionary<string, long>>
    {
        private readonly string[] _keys;

        public GetCompareExchangeIndexOperation(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key), "The key argument must have value");
            _keys = new[] {key};
        }

        public GetCompareExchangeIndexOperation(string[] keys)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentNullException(nameof(keys), "The keys argument must have value");
            _keys = keys;
        }

        public RavenCommand<Dictionary<string, long>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context,
            HttpCache cache)
        {
            return new GetCompareExchangeIndexCommand(_keys);
        }

        private class GetCompareExchangeIndexCommand : RavenCommand<Dictionary<string, long>>
        {
            private readonly string[] _keys;

            public GetCompareExchangeIndexCommand(string[] keys)
            {
                _keys = keys;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/cmpxchg/indexes")
                    .Append("?key=").Append(Uri.EscapeDataString(_keys[0]));

                for (var index = 1; index < _keys.Length; index++)
                {
                    var key = _keys[index];
                    pathBuilder.Append("&key=")
                        .Append(Uri.EscapeDataString(key));
                }


                url = pathBuilder.ToString();
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get,
                };
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = new Dictionary<string, long>();

                foreach (var propertyName in response.GetPropertyNames())
                {
                    if (response.TryGet(propertyName, out long val))
                    {
                        Result[propertyName] = val;
                    }
                }

                if (Result.Values.Count != _keys.Length)
                {
                    ThrowInvalidCountOfItems();
                }
            }

            private void ThrowInvalidCountOfItems()
            {
                throw new InvalidOperationException(
                    $"The result of {nameof(GetCompareExchangeIndexOperation)} has {Result.Values.Count} entries, while the request has only {_keys.Length}.");
            }
        }
    }
}
