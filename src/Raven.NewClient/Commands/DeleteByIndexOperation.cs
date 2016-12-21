using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class DeleteByIndexOperation
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<DeleteByIndexOperation>("Raven.NewClient.Client");

        public DeleteByIndexOperation()
        {
        }

        protected void LogDeleteByIndex(string indexName)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Delete by '{indexName}' index");
        }

        public DeleteByIndexCommand CreateRequest(string indexName, IndexQuery queryToDelete, QueryOperationOptions options, DocumentStore documentStore)
        {
            return new DeleteByIndexCommand()
            {
                IndexName = indexName,
                QueryToDelete = queryToDelete,
                Options = options,
            };
        }


    }
}