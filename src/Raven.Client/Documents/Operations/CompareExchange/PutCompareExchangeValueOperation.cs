﻿using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    /// <summary>
    /// Operation to insert or update a compare exchange value in the RavenDB database.
    /// A compare exchange value is a distributed key-value pair used for coordination between nodes in a cluster.
    /// </summary>
    /// <typeparam name="T">The type of the value to be stored in the compare exchange.</typeparam>
    public sealed class PutCompareExchangeValueOperation<T> : IOperation<CompareExchangeResult<T>>
    {
        private readonly string _key;
        private readonly T _value;
        private readonly long _index;
        private readonly IMetadataDictionary _metadata;

        /// <summary>
        /// Operation to insert or update a compare exchange value in the RavenDB database.
        /// Initializes a new instance of the <see cref="PutCompareExchangeValueOperation{T}"/> class.
        /// </summary>
        /// <param name="key">The key associated with the compare exchange value.</param>
        /// <param name="value">The value to be stored in the compare exchange.</param>
        /// <param name="index">The index used for optimistic concurrency control. Pass <c>0</c> to create a new compare exchange.</param>
        /// <param name="metadata">Optional metadata associated with the compare exchange value.</param>
        /// <remarks>
        /// The index must be set to <c>0</c> for new compare exchange entries. For updates, the current index of the existing entry must be provided.
        /// </remarks>
        public PutCompareExchangeValueOperation(string key, T value, long index, IMetadataDictionary metadata = null)
        {
            _key = key;
            _value = value;
            _index = index;
            _metadata = metadata;
        }

        public RavenCommand<CompareExchangeResult<T>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PutCompareExchangeValueCommand(_key, _value, _index, _metadata, conventions);
        }

        private sealed class PutCompareExchangeValueCommand : RavenCommand<CompareExchangeResult<T>>, IRaftCommand
        {
            private readonly string _key;
            private readonly T _value;
            private readonly long _index;
            private readonly DocumentConventions _conventions;
            private readonly IMetadataDictionary _metadata;

            public PutCompareExchangeValueCommand(string key, T value, long index, IMetadataDictionary metadata = null, DocumentConventions conventions = null)
            {
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentNullException(nameof(key), "The key argument must have value");
                if (index < 0)
                    throw new InvalidDataException("Index must be a non-negative number");

                _key = key;
                _value = value;
                _index = index;
                _metadata = metadata;
                _conventions = conventions ?? DocumentConventions.Default;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/cmpxchg?key={Uri.EscapeDataString(_key)}&index={_index}";
                var djv = new DynamicJsonValue
                {
                    [Constants.CompareExchange.ObjectFieldName] = CompareExchangeValueBlittableJsonConverter.ConvertToBlittable(_value, _conventions, ctx)
                };

                if (_metadata != null)
                {
                    var metadata = ClusterTransactionOperationsBase.CompareExchangeSessionValue.PrepareMetadataForPut(_key, _metadata, _conventions, ctx);
                    djv[Constants.Documents.Metadata.Key] = metadata;
                }
                var blittable = ctx.ReadObject(djv, _key);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, blittable).ConfigureAwait(false), _conventions)
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
