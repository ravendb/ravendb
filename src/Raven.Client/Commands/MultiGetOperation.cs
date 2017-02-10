using System.Collections.Generic;
using Raven.Client.Data;
using Raven.Client.Document;

namespace Raven.Client.Commands
{
    public class MultiGetOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;

        public MultiGetOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public MultiGetCommand CreateRequest(List<GetRequest> requests)
        {
            return new MultiGetCommand(_session.Context, _session.RequestExecuter.Cache, requests);
        }

        public void SetResult(BlittableArrayResult result)
        {
        }
    }
}