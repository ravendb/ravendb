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
    /// <summary>
    /// Operation to retrieve multiple compare exchange values from a database in a single request.
    /// A compare exchange is a key-value pair is a distributed coordination mechanism that ensures a consistent state across the cluster.
    /// </summary>
    /// <typeparam name="T">The type of the value associated with the compare exchange keys.</typeparam>
    public sealed class GetCompareExchangeValuesOperation<T> : IOperation<Dictionary<string, CompareExchangeValue<T>>>
    {
        private readonly string[] _keys;

        private readonly string _startWith;
        private readonly int? _start;
        private readonly int? _pageSize;

        private readonly bool _materializeMetadata;

        /// <summary>
        /// Operation to retrieve multiple compare exchange values from a database in a single request.
        /// A compare exchange is a key-value pair is a distributed coordination mechanism that ensures a consistent state across the cluster.
        /// Initializes a new instance of the <see cref="GetCompareExchangeValuesOperation{T}"/> class.
        /// Retrieves compare exchange values for the specified keys.
        /// </summary>
        /// <param name="keys">The array of keys for the compare exchange values to retrieve.</param>
        /// <exception cref="ArgumentNullException">Thrown when the <paramref name="keys"/> is null or empty.</exception>
        public GetCompareExchangeValuesOperation(string[] keys)
            : this(keys, materializeMetadata: true)
        {
        }

        internal GetCompareExchangeValuesOperation(string[] keys, bool materializeMetadata)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentNullException(nameof(keys));

            _keys = keys;
            _materializeMetadata = materializeMetadata;
        }

        public GetCompareExchangeValuesOperation(string startWith, int? start = null, int? pageSize = null)
        {
            _startWith = startWith;
            _start = start;
            _pageSize = pageSize;
            _materializeMetadata = true;
        }

        public RavenCommand<Dictionary<string, CompareExchangeValue<T>>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetCompareExchangeValuesCommand(this, _materializeMetadata, conventions);
        }

        private sealed class GetCompareExchangeValuesCommand : RavenCommand<Dictionary<string, CompareExchangeValue<T>>>
        {
            private readonly GetCompareExchangeValuesOperation<T> _operation;
            private readonly bool _materializeMetadata;
            private readonly DocumentConventions _conventions;

            public GetCompareExchangeValuesCommand(GetCompareExchangeValuesOperation<T> operation, bool materializeMetadata, DocumentConventions conventions)
            {
                _operation = operation;
                _materializeMetadata = materializeMetadata;
                _conventions = conventions;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/cmpxchg?");

                if (_operation._keys != null)
                {
                    foreach (var key in _operation._keys)
                    {
                        pathBuilder.Append("&key=").Append(Uri.EscapeDataString(key));
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(_operation._startWith) == false)
                        pathBuilder.Append("&startsWith=").Append(Uri.EscapeDataString(_operation._startWith));
                    if (_operation._start.HasValue)
                        pathBuilder.Append("&start=").Append(_operation._start);
                    if (_operation._pageSize.HasValue)
                        pathBuilder.Append("&pageSize=").Append(_operation._pageSize);
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
                Result = CompareExchangeValueResultParser<T>.GetValues(response, _materializeMetadata, _conventions);
            }
        }
    }
}
