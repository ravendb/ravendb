using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    public class GetCompareExchangeIndexResult
    {
        public long[] Indexes;
    }

    public class GetCompareExchangeIndexOperation : IOperation<GetCompareExchangeIndexResult>
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
            if (keys != null && keys.Length > 0)
                throw new ArgumentNullException(nameof(keys), "The keys argument must have value");
            _keys = keys;
        }

        public RavenCommand<GetCompareExchangeIndexResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context,
            HttpCache cache)
        {
            return new GetCompareExchangeIndexCommand(_keys);
        }

        private class GetCompareExchangeIndexCommand : RavenCommand<GetCompareExchangeIndexResult>
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
                    .Append("/cmpxchg/indexes?")
                    .Append("keys=")
                    .Append(Uri.EscapeDataString(string.Join(",", _keys)));

                url = pathBuilder.ToString();
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get,
                };
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.GetCompareExchangeIndexResult(response);
                if (Result.Indexes.Length != _keys.Length)
                {
                    throw new InvalidOperationException(
                        $"The result of {nameof(GetCompareExchangeIndexOperation)} has {Result.Indexes.Length} entries, while the request has only {_keys.Length}.");
                }
            }
        }
    }
}
