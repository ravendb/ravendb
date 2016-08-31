using System.Linq;
using System.Text;
using Raven.Client.Documents.Commands;
using Sparrow.Logging;

namespace Raven.Client.Documents.SessionOperations
{
    public class BatchOperation
    {
        private readonly Document.InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggerSetup.Instance.GetLogger<LoadOperation>("Raven.Client");

        public Document.InMemoryDocumentSessionOperations.SaveChangesData Data;

        public BatchOperation(Document.InMemoryDocumentSessionOperations session)
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
                    sb.AppendFormat("\t{0} {1}", commandData.Method, commandData.Id).AppendLine();
                }
                _logger.Info(sb.ToString());
            }
        }

        public BatchCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            LogBatch();
            return new BatchCommand
            {
                // Commands = Data.Commands.Select(command => command.ToJson()).ToList()
            };
        }
    }
}