using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations
{
    internal class MoreLikeThisOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly MoreLikeThisQuery _query;

        private MoreLikeThisQueryResult _result;

        public MoreLikeThisOperation(InMemoryDocumentSessionOperations session, MoreLikeThisQuery query)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _query = query ?? throw new ArgumentNullException(nameof(query));

            if (_query.Query == null)
                throw new ArgumentNullException(nameof(query.Query));
        }

        public MoreLikeThisCommand CreateRequest()
        {
            _session.IncrementRequestCount();

            return new MoreLikeThisCommand(_session.Conventions, _query);
        }

        public void SetResult(MoreLikeThisQueryResult result)
        {
            _result = result;
        }

        public List<T> Complete<T>()
        {
            _session.RegisterIncludes(_result.Includes);

            var list = new List<T>();
            foreach (BlittableJsonReaderObject document in _result.Results)
            {
                var metadata = document.GetMetadata();

                metadata.TryGetId(out var id);

                list.Add(QueryOperation.Deserialize<T>(id, document, metadata, projectionFields: null, disableEntitiesTracking: false, session: _session));
            }

            _session.RegisterMissingIncludes(_result.Results, _result.Includes, _query.Includes);

            return list;
        }
    }
}
