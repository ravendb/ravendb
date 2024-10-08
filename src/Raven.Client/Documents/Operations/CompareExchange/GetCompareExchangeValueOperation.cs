using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    /// <summary>
    /// Operation to retrieve a compare exchange value from a RavenDB database.
    /// A compare exchange value is a distributed key-value pair used for coordinating actions across the cluster.
    /// </summary>
    /// <typeparam name="T">The type of the value associated with the compare exchange key.</typeparam>
    public sealed class GetCompareExchangeValueOperation<T> : IOperation<CompareExchangeValue<T>>
    {
        private readonly string _key;

        private readonly bool _materializeMetadata;

        private readonly string _nodeTag;

        /// <summary>
        /// Operation to retrieve a compare exchange value from a RavenDB database.
        /// A compare exchange value is a distributed key-value pair used for coordinating actions across the cluster.
        /// Initializes a new instance of the <see cref="GetCompareExchangeValueOperation{T}"/> class.
        /// Retrieves the compare exchange value for the specified key.
        /// </summary>
        /// <typeparam name="T">The type of the value associated with the compare exchange key.</typeparam>
        /// <param name="key">The key of the compare exchange value to retrieve.</param>
        /// <exception cref="ArgumentNullException">Thrown when the <paramref name="key"/> is null or empty.</exception>
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

        private sealed class GetCompareExchangeValueCommand : RavenCommand<CompareExchangeValue<T>>
        {
            private readonly string _key;
            private readonly bool _materializeMetadata;
            private readonly DocumentConventions _conventions;

            internal GetCompareExchangeValueCommand(string key, bool materializeMetadata, DocumentConventions conventions, string selectedNodeTag)
                : this(key, materializeMetadata, conventions)
            {
                SelectedNodeTag = selectedNodeTag;
            }

            private GetCompareExchangeValueCommand(string key, bool materializeMetadata, DocumentConventions conventions)
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
