using System;
using System.Text;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyGetCompareExchangeValuesOperation<T> : ILazyOperation
    {
        private readonly string[] _keys;
        private readonly DocumentConventions _conventions;
        private readonly JsonOperationContext _context;

        public LazyGetCompareExchangeValuesOperation(string[] keys, DocumentConventions conventions, JsonOperationContext context)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentNullException(nameof(keys));

            _keys = keys;
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public object Result { get; private set; }

        public QueryResult QueryResult => throw new NotImplementedException();

        public bool RequiresRetry { get; private set; }

        public GetRequest CreateRequest(JsonOperationContext ctx)
        {
            var queryBuilder = new StringBuilder("?");

            foreach (var key in _keys)
                queryBuilder.Append("&key=").Append(Uri.EscapeDataString(key));

            return new GetRequest
            {
                Url = "/cmpxchg",
                Method = HttpMethods.Get,
                Query = queryBuilder.ToString()
            };
        }

        public void HandleResponse(GetResponse response)
        {
            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            if (response.Result != null)
                Result = CompareExchangeValueResultParser<T>.GetValues((BlittableJsonReaderObject)response.Result, _conventions);
        }
    }
}
