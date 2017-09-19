using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class RawClusterValueResult
    {
        public BlittableJsonReaderObject Value;
        public long Index;
    }

    public class CmpXchgResult<T>
    {
        public T Value;
        public long Index;


        public static CmpXchgResult<T> ParseFromBlittable(BlittableJsonReaderObject response, DocumentConventions conventions)
        {
            if (response.TryGet(nameof(RawClusterValueResult.Index), out long index) == false)
                throw new InvalidDataException("Response is invalid.");

            response.TryGet(nameof(RawClusterValueResult.Value), out BlittableJsonReaderObject raw);

            T result;
            object val = null;
            raw?.TryGet("Object", out val);

            if (val == null)
            {
                return new CmpXchgResult<T>
                {
                    Index = index,
                    Value = default(T)
                };
            }
            if (val is BlittableJsonReaderObject obj)
            {
                result = (T)EntityToBlittable.ConvertToEntity(typeof(T), "cluster-value", obj, conventions);
            }
            else
            {
                raw.TryGet("Object", out result);
            }

            return new CmpXchgResult<T>
            {
                Index = index,
                Value = result
            };
        }
    }

    public class GetCompareExchangeValueAsync<T> : IOperation<CmpXchgResult<T>>
    {
        private readonly string _key;

        public GetCompareExchangeValueAsync(string key)
        {
            _key = key;
        }

        public RavenCommand<CmpXchgResult<T>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetCompareExchangeValueAsyncCommand(_key, conventions);
        }

        private class GetCompareExchangeValueAsyncCommand : RavenCommand<CmpXchgResult<T>>
        {
            private readonly string _key;
            private readonly DocumentConventions _conventions;
            
            public GetCompareExchangeValueAsyncCommand(string key, DocumentConventions conventions = null)
            {
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentNullException(nameof(key), "The key argument must have value");

                _key = key;
                _conventions = conventions ?? DocumentConventions.Default;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/cmpxchg?key={_key}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get,
                };
                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = CmpXchgResult<T>.ParseFromBlittable(response, _conventions);
            }
        }

    }

    public class CompareExchangeAsync<T> : IOperation<CmpXchgResult<T>>
    {
        private readonly string _key;
        private readonly T _value;
        private readonly long _index;

        public CompareExchangeAsync(string key, T value, long index)
        {
            _key = key;
            _value = value;
            _index = index;
        }

        public RavenCommand<CmpXchgResult<T>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new CompareExchangeAsyncCommand(_key, _value, _index, conventions);
        }

        private class CompareExchangeAsyncCommand : RavenCommand<CmpXchgResult<T>>
        {
            private readonly string _key;
            private readonly T _value;
            private readonly long _index;
            private readonly DocumentConventions _conventions;

            public CompareExchangeAsyncCommand(string key, T value, long index, DocumentConventions conventions = null)
            {
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentNullException(nameof(key), "The key argument must have value");
                if (index < 0)
                    throw new InvalidDataException("Index must be a non-negative number");

                _key = key;
                _value = value;
                _index = index;
                _conventions = conventions ?? DocumentConventions.Default;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/cmpxchg?key={_key}&index={_index}";
                //var tuple = ("Object", _value);
                var tuple = new Dictionary<string, T>
                {
                    ["Object"] = _value
                };
                var blit = EntityToBlittable.ConvertEntityToBlittable(tuple,_conventions,ctx);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, blit);
                    })
                };
                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = CmpXchgResult<T>.ParseFromBlittable(response, _conventions);
            }
        }
    }
}
