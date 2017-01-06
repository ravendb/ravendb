using System;
using System.Collections.Generic;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
            return new MultiGetCommand()
            {
                GetCommands = requests,
                Context = _session.Context,
                IsReadRequest = false
            };
        }

        public void SetResult(BlittableArrayResult result)
        {
        }
    }
}