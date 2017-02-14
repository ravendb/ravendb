using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations
{
    public class GetRevisionOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly string _id;
        private readonly int _start;
        private readonly int _pageSize;

        private BlittableArrayResult _result;

        public GetRevisionOperation(InMemoryDocumentSessionOperations session, string id, int start, int pageSize)
        {
            if (session == null)
                throw new ArgumentNullException(nameof(session));
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            _session = session;
            _id = id;
            _start = start;
            _pageSize = pageSize;
        }

        public GetRevisionCommand CreateRequest()
        {
            return new GetRevisionCommand(_id, _start, _pageSize);
        }

        public void SetResult(BlittableArrayResult result)
        {
            _result = result;
        }

        public List<T> Complete<T>()
        {
            var results = new List<T>(_result.Results.Length);
            for (var i = 0; i < _result.Results.Length; i++)
            {
                var document = (BlittableJsonReaderObject)_result.Results[i];
                var metadata = document.GetMetadata();
                var id = metadata.GetId();

                results.Add((T)_session.ConvertToEntity(typeof(T), id, document));
            }

            return results;
        }
    }
}