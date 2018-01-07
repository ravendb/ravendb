using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetCompareExchangeStartsWithOperation<T> : IOperation<List<(string Key, long Index, T Value)>>
    {
        private readonly string _keyPrefix;
        private readonly int? _page;
        private readonly int? _size;

        public RavenCommand<List<(string Key, long Index, T Value)>> GetCommand(IDocumentStore store, DocumentConventions conventions, 
            JsonOperationContext context, HttpCache cache)
        {
            return new ListCompareExchangeValuesCommand(_keyPrefix, _page, _size, conventions);
        }

        public GetCompareExchangeStartsWithOperation(string keyPrefix, int? page = null, int? size = null)
        {
            _keyPrefix = keyPrefix;
            _page = page;
            _size = size;
        }

        private class ListCompareExchangeValuesCommand : RavenCommand<List<(string Key, long Index, T Value)>>
        {
            private readonly string _keyPrefix;
            private readonly int? _page;
            private readonly int? _size;
            private readonly DocumentConventions _conventions;

            public ListCompareExchangeValuesCommand(string keyPrefix, int? page, int? size, DocumentConventions conventions)
            {
                _keyPrefix = keyPrefix;
                _page = page;
                _size = size;
                _conventions = conventions;
            }

            public override bool IsReadRequest => true;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/cmpxchg/list?startsWith={_keyPrefix}";
                
                if (_page != null)
                {
                    url += $"&start={_page}";
                }
                
                if (_size != null)
                {
                    url += "&pageSize={_size}";
                }
                
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Get,
                };
                return request;
            }

            private IEnumerable<(string Key, long Index, T Value)> GetResult(BlittableJsonReaderArray array)
            {
                if (array == null)
                    yield break;
                
                foreach (BlittableJsonReaderObject item in array)
                {
                    if (item == null)
                        continue;

                    item.TryGet("Index", out long index);
                    item.TryGet("Value", out BlittableJsonReaderObject raw);
                    item.TryGet("Key", out string key);

                    if (typeof(T).GetTypeInfo().IsPrimitive || typeof(T) == typeof(string))
                    {
                        // simple
                        T value = default(T);
                        raw?.TryGet<T>("Object", out value);
                        yield return (key, index, value);
                    }
                    else
                    {
                        BlittableJsonReaderObject val = null;
                        raw?.TryGet("Object", out val);
                        if (val == null)
                        {
                            yield return (key, index, default(T));
                        }
                        else
                        {
                            var convereted = EntityToBlittable.ConvertToEntity(typeof(T), null, val, _conventions);
                            yield return (key, index, (T)convereted);
                        }
                    }
                }
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                response.TryGet("Results", out BlittableJsonReaderArray array);
                Result = GetResult(array).ToList();
            }
        }
    }
}
