using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetCompareExchangeOperation<T> : IOperation<List<(string Key, long Index, T Value)>>
    {
        private readonly string[] _keys;

        private readonly string _startWith;
        private readonly int? _start;
        private readonly int? _pageSize;
        
        public GetCompareExchangeOperation(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key), "The key argument must have value");
            _keys = new []{key};
        }
        
        public GetCompareExchangeOperation(string[] keys)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentNullException(nameof(keys));

            _keys = keys;
        }
        
        public GetCompareExchangeOperation(string startWith, int start, int pageSize)
        {
            _startWith = startWith;
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<List<(string Key, long Index, T Value)>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetCompareExchangeCommand(this, conventions);
        }

        private class GetCompareExchangeCommand : RavenCommand<List<(string Key, long Index, T Value)>>
        {
            private readonly GetCompareExchangeOperation<T> _operation;
            private readonly DocumentConventions _conventions;

            public GetCompareExchangeCommand(GetCompareExchangeOperation<T> operation, DocumentConventions conventions)
            {
                _operation = operation;
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
                if (response.TryGet("Results", out BlittableJsonReaderArray items) == false)
                    throw new InvalidDataException("Response is invalid. Results is missing.");

                var results = new List<(string Key, long Index, T Value)>();
                foreach (BlittableJsonReaderObject item in items)
                {
                    if (item == null)
                        throw new InvalidDataException("Response is invalid. Item is null.");
                    
                    if (item.TryGet("Key", out string key) == false)
                        throw new InvalidDataException("Response is invalid. Key is missing.");
                    if (item.TryGet("Index", out long index) == false)
                        throw new InvalidDataException("Response is invalid. Index is missing.");
                    if (item.TryGet("Value", out BlittableJsonReaderObject raw) == false)
                        throw new InvalidDataException("Response is invalid. Value is missing.");

                    if (typeof(T).GetTypeInfo().IsPrimitive || typeof(T) == typeof(string))
                    {
                        // simple
                        T value = default(T);
                        raw?.TryGet("Object", out value);
                        results.Add((key, index, value));
                    }
                    else
                    {
                        BlittableJsonReaderObject val = null;
                        raw?.TryGet("Object", out val);
                        if (val == null)
                        {
                            results.Add((key, index, default(T)));
                        }
                        else
                        {
                            var convereted = EntityToBlittable.ConvertToEntity(typeof(T), null, val, _conventions);
                            results.Add((key, index, (T)convereted));
                        }
                    }
                }
                
                Result = results;
            }
        }
    }
}
