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
    public class GetRawUniqueValueResult
    {
        public BlittableJsonReaderObject Value;
        public long Index;
    }

    public class GetUniqueValueResult<T>
    {
        public T Value;
        public long Index;
    }

    public class GetUniqueValue<T> : IServerOperation<GetUniqueValueResult<T>>
    {
        private readonly string _key;

        public GetUniqueValue(string key)
        {
            _key = key;
        }

        public RavenCommand<GetUniqueValueResult<T>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetUniqueValueCommand(_key, conventions);
        }

        private class GetUniqueValueCommand : RavenCommand<GetUniqueValueResult<T>>
        {
            private readonly string _key;
            private readonly DocumentConventions _conventions;
            
            public GetUniqueValueCommand(string key, DocumentConventions conventions = null)
            {
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentNullException(nameof(key), "The key argument must have value");

                _key = key;
                _conventions = conventions ?? DocumentConventions.Default;
                ResponseType = RavenCommandResponseType.Raw;
            }

            public override bool IsReadRequest { get; }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/unique?key={_key}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get,
                };
                return request;
            }

            public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
            {
                var json = context.ReadForMemory(stream, "response/object");

                if (json.TryGet(nameof(GetRawUniqueValueResult.Index), out long index) == false)
                    ThrowInvalidResponse();

                json.TryGet(nameof(GetRawUniqueValueResult.Value), out BlittableJsonReaderObject raw);

                T result;
                object val = null;
                raw?.TryGet("Object", out val);

                if (val == null)
                {
                    Result = new GetUniqueValueResult<T>
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

                Result = new GetUniqueValueResult<T>
                {
                    Index = index,
                    Value = result
                };
            }
        }
    }

    public class UniqueValueOperation<T> : IServerOperation
    {
        private readonly string _key;
        private readonly T _value;
        private readonly long _index;

        public UniqueValueOperation(string key, T value, long index)
        {
            _key = key;
            _value = value;
            _index = index;
        }

        public RavenCommand GetCommand( DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutUniqueValueCommand(_key, _value, _index, conventions);
        }

        private class PutUniqueValueCommand : RavenCommand
        {
            private readonly string _key;
            private readonly T _value;
            private readonly long _index;
            private readonly DocumentConventions _conventions;

            public PutUniqueValueCommand(string key, T value, long index, DocumentConventions conventions = null)
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
                url = $"{node.Url}/unique?key={_key}&index={_index}";
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
