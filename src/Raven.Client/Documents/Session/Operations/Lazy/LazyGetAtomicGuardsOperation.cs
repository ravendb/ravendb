using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal sealed class LazyGetAtomicGuardsOperation : ILazyOperation
    {
        private readonly ClusterTransactionOperationsBase _clusterSession;
        private readonly DocumentConventions _conventions;
        private readonly string[] _documentIds;
        private readonly bool _single;
        private Dictionary<string, string> _result;

        public LazyGetAtomicGuardsOperation(ClusterTransactionOperationsBase clusterSession, DocumentConventions conventions, string[] documentIds, bool single)
        {
            _clusterSession = clusterSession ?? throw new ArgumentNullException(nameof(clusterSession));
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _documentIds = documentIds ?? throw new ArgumentNullException(nameof(documentIds));
            _single = single;
        }

        public object Result { get; private set; }

        public QueryResult QueryResult => throw new NotImplementedException();

        public bool RequiresRetry { get; private set; }

        public GetRequest CreateRequest(JsonOperationContext ctx)
        {
            var keys = _clusterSession.GetAtomicGuardKeysToFetch(_documentIds, out _result);
            if (keys == null)
            {
                SetResult();
                return null;
            }

            var query = new StringBuilder("?");
            foreach (var key in keys)
                query.Append("&key=").Append(Uri.EscapeDataString(key));

            return new GetRequest
            {
                Url = "/cmpxchg",
                Method = HttpMethods.Get,
                Query = query.ToString()
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
                _clusterSession.TrackAtomicGuards(
                    CompareExchangeValueResultParser<BlittableJsonReaderObject>.GetValues((BlittableJsonReaderObject)response.Result, materializeMetadata: false, _conventions),
                    _result);
            }

            SetResult();
        }

        private void SetResult()
        {
            Result = _single ? _result[_documentIds[0]] : _result;
        }
    }
}
