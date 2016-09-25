using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.SessionOperations.Commands;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Client.Documents.SessionOperations
{
    public class BatchOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.Client");

        public InMemoryDocumentSessionOperations.SaveChangesData Data;

        public BatchOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        protected void LogBatch()
        {
            if (_logger.IsInfoEnabled)
            {
                var sb = new StringBuilder()
                    .AppendFormat("Saving {0} changes to {1}", Data.Commands.Count, _session.StoreIdentifier)
                    .AppendLine();
                foreach (var commandData in Data.Commands)
                {
                    sb.AppendFormat("\t{0} {1}", commandData["Method"], commandData["Key"]).AppendLine();
                }
                _logger.Info(sb.ToString());
            }
        }

        public BatchCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            LogBatch();
            return new BatchCommand()
            {
                Commands = _session.PrepareForSaveChanges(),
                Context = _session.Context
            };
        }

        public void SetResult(BatchResult result)
        {
            //TODO
        }
    }
}