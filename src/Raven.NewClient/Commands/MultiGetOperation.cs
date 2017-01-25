using System.Collections.Generic;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class MultiGetOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<MultiGetOperation>("Raven.Client");

        public MultiGetOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        protected void LogMultiGet()
        {
           //TODO
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