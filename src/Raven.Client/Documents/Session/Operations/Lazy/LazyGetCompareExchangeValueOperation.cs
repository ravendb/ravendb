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
    internal class LazyGetCompareExchangeValueOperation<T> : ILazyOperation
    {
        private readonly string _key;
        private readonly DocumentConventions _conventions;
        private readonly JsonOperationContext _context;

        public LazyGetCompareExchangeValueOperation(string key, DocumentConventions conventions, JsonOperationContext context)
        {
            _key = key ?? throw new ArgumentNullException(nameof(key));
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public object Result { get; private set; }

        public QueryResult QueryResult => throw new NotImplementedException();

        public bool RequiresRetry { get; private set; }

        public GetRequest CreateRequest(JsonOperationContext ctx)
        {
            var queryBuilder = new StringBuilder("?key=")
                .Append(_key);

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
                Result = CompareExchangeValueResultParser<T>.GetValue((BlittableJsonReaderObject)response.Result, _conventions);
        }
    }
}
