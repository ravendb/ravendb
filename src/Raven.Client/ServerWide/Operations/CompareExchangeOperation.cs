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
    public class RawClusterValueResult
    {
        public BlittableJsonReaderObject Value;
        public long Index;
    }

    public class ClusterValueResult<T>
    {
        public T Value;
        public long Index;
    }

    public class GetClusterValue<T> : IServerOperation<ClusterValueResult<T>>
    {
        private readonly string _key;

        public GetClusterValue(string key)
        {
            _key = key;
        }

        public RavenCommand<ClusterValueResult<T>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetClusterValueCommand(_key, conventions);
        }

        private class GetClusterValueCommand : RavenCommand<ClusterValueResult<T>>
        {
            private readonly string _key;
            private readonly DocumentConventions _conventions;
            
            public GetClusterValueCommand(string key, DocumentConventions conventions = null)
            {
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentNullException(nameof(key), "The key argument must have value");

                _key = key;
                _conventions = conventions ?? DocumentConventions.Default;
            }

            public override bool IsReadRequest => true;

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

                if (response.TryGet(nameof(RawClusterValueResult.Index), out long index) == false)
                    ThrowInvalidResponse();

                response.TryGet(nameof(RawClusterValueResult.Value), out BlittableJsonReaderObject raw);

                T result;
                object val = null;
                raw?.TryGet("Object", out val);

                if (val == null)
                {
                    Result = new ClusterValueResult<T>
                    {
                        Index = index,
                        Value = default(T)
                    };
                    return;
                }
                if (val is BlittableJsonReaderObject obj)
                {
                    result = (T)EntityToBlittable.ConvertToEntity(typeof(T), "cluster-value", obj, _conventions);
                }
                else
                {
                    raw.TryGet("Object", out result);
                }

                Result = new ClusterValueResult<T>
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

            public override bool IsReadRequest => false;

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
