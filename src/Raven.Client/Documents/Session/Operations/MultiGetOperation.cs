using System.Collections.Generic;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Json;

namespace Raven.Client.Documents.Session.Operations
{
    internal class MultiGetOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;

        public MultiGetOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public MultiGetCommand CreateRequest(List<GetRequest> requests)
        {
            return new MultiGetCommand(_session.RequestExecutor, requests);
        }

        public void SetResult(BlittableArrayResult result)
        {
        }
    }
}
