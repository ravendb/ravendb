using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetRawCompareValueResult
    {
        public BlittableJsonReaderObject Value;
        public long Index;
    }

    public class GetCompareValueResult<T>
    {
        public T Value;
        public long Index;
    }

    public class GetCompareValue<T> : IServerOperation<GetCompareValueResult<T>>
    {
        private readonly string _key;

        public GetCompareValue(string key)
        {
            _key = key;
        }

        public RavenCommand<GetCompareValueResult<T>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCompareValueCommand(_key, conventions);
        }

        private class GetCompareValueCommand : RavenCommand<GetCompareValueResult<T>>
        {
            private readonly string _key;
            private readonly DocumentConventions _conventions;
            
            public GetCompareValueCommand(string key, DocumentConventions conventions = null)
            {
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentNullException(nameof(key), "The key argument must have value");

                _key = key;
                _conventions = conventions ?? DocumentConventions.Default;
            }

            public override bool IsReadRequest { get; }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/cluster/cmpxchg?key={_key}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get,
                };
                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {

                if (response.TryGet(nameof(GetRawCompareValueResult.Index), out long index) == false)
                    ThrowInvalidResponse();

                response.TryGet(nameof(GetRawCompareValueResult.Value), out BlittableJsonReaderObject raw);

                T result;
                object val = null;
                raw?.TryGet("Object", out val);

                if (val == null)
                {
                    Result = new GetCompareValueResult<T>
                    {
                        Index = index,
                        Value = default(T)
                    };
                    return;
                }
                var type = val.GetType();
                if (typeof(BlittableJsonReaderObject) == type)
                {
                    result = (T)EntityToBlittable.ConvertToEntity(typeof(T), "asd", (BlittableJsonReaderObject)val, _conventions);
                }
                else
                {
                    raw.TryGet("Object", out result);
                }

                Result = new GetCompareValueResult<T>
                {
                    Index = index,
                    Value = result
                };
            }
        }
    }

    public class CompareExchangeOperation<T> : IServerOperation
    {
        private readonly string _key;
        private readonly T _value;
        private readonly long _index;

        public CompareExchangeOperation(string key, T value, long index)
        {
            _key = key;
            _value = value;
            _index = index;
        }

        public RavenCommand GetCommand( DocumentConventions conventions, JsonOperationContext context)
        {
            return new CompareExchangeCommand(_key, _value, _index, conventions);
        }

        private class CompareExchangeCommand : RavenCommand
        {
            private readonly string _key;
            private readonly T _value;
            private readonly long _index;
            private readonly DocumentConventions _conventions;

            public CompareExchangeCommand(string key, T value, long index, DocumentConventions conventions = null)
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

            public override bool IsReadRequest { get; }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/cluster/cmpxchg?key={_key}&index={_index}";
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
        }
    }
}
