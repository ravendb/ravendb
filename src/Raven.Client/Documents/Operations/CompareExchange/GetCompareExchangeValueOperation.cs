using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    public class GetCompareExchangeValueOperation<T> : IOperation<CompareExchangeValue<T>>
    {
        private readonly string _key;

        private readonly bool _materializeMetadata;

        private readonly string _nodeTag;

        public GetCompareExchangeValueOperation(string key)
            : this(key, materializeMetadata: true)
        {
        }

        internal GetCompareExchangeValueOperation(string key, string nodeTag)
            : this(key, materializeMetadata: true)
        {
            _nodeTag = nodeTag;
        }

        internal GetCompareExchangeValueOperation(string key, bool materializeMetadata)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key), "The key argument must have value");
            _key = key;
            _materializeMetadata = materializeMetadata;
        }

        public RavenCommand<CompareExchangeValue<T>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetCompareExchangeValueCommand(_key, _materializeMetadata, conventions, _nodeTag);
        }

        private class GetCompareExchangeValueCommand : RavenCommand<CompareExchangeValue<T>>
        {
            private readonly string _key;
            private readonly bool _materializeMetadata;
            private readonly DocumentConventions _conventions;

            internal GetCompareExchangeValueCommand(string key, bool materializeMetadata, DocumentConventions conventions, string selectedNodeTag)
                : this(key, materializeMetadata, conventions)
            {
                SelectedNodeTag = selectedNodeTag;
            }

            public GetCompareExchangeValueCommand(string key, bool materializeMetadata, DocumentConventions conventions)
            {
                _key = key;
                _materializeMetadata = materializeMetadata;
                _conventions = conventions;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/cmpxchg?key=")
                    .Append(Uri.EscapeDataString(_key));

                url = pathBuilder.ToString();
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get,
                };
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = CompareExchangeValueResultParser<T>.GetValue(response, _materializeMetadata, _conventions);
            }
        }
    }
}
