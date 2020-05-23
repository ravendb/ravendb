using System;
using System.Diagnostics;
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
        private readonly ClusterTransactionOperationsBase _clusterSession;
        private readonly DocumentConventions _conventions;
        private readonly string[] _keys;

        public LazyGetCompareExchangeValuesOperation(ClusterTransactionOperationsBase clusterSession, DocumentConventions conventions, string[] keys)
        {
            if (keys == null || keys.Length == 0)
                throw new ArgumentNullException(nameof(keys));
            _clusterSession = clusterSession ?? throw new ArgumentNullException(nameof(clusterSession));
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _keys = keys;
        }

        public object Result { get; private set; }

        public QueryResult QueryResult => throw new NotImplementedException();

        public bool RequiresRetry { get; private set; }

        public GetRequest CreateRequest(JsonOperationContext ctx)
        {
            StringBuilder queryBuilder = null;

            foreach (var key in _keys)
            {
                if (_clusterSession.IsTracked(key))
                    continue;

                if (queryBuilder == null)
                    queryBuilder = new StringBuilder("?");

                queryBuilder.Append("&key=").Append(Uri.EscapeDataString(key));
            }

            if (queryBuilder == null)
            {
                Result = _clusterSession.GetCompareExchangeValuesFromSessionInternal<T>(_keys, out _);
                return null;
            }

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
            {
                foreach (var kvp in CompareExchangeValueResultParser<BlittableJsonReaderObject>.GetValues((BlittableJsonReaderObject)response.Result, materializeMetadata: false, _conventions))
                {
                    if (kvp.Value == null)
                        continue;

                    _clusterSession.RegisterCompareExchangeValue(kvp.Value);
                }
            }

            foreach (var key in _keys)
            {
                if (_clusterSession.IsTracked(key))
                    continue;

                _clusterSession.RegisterMissingCompareExchangeValue(key);
            }

            Result = _clusterSession.GetCompareExchangeValuesFromSessionInternal<T>(_keys, out var notTrackedKeys);
            Debug.Assert(_clusterSession._session.NoTracking || notTrackedKeys == null, "missingKeys == null");
        }
    }
}
