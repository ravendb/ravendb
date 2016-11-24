using Raven.NewClient.Client.Document;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class DeleteDatabaseOperation
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<DeleteDatabaseOperation>("Raven.NewClient.Client");

        public InMemoryDocumentSessionOperations.SaveChangesData Data;

        public DeleteDatabaseOperation()
        {
        }

        protected void LogBatch()
        {
            if (_logger.IsInfoEnabled)
            {
               //TODO - Efrat
            }
        }

        public DeleteDatabaseCommand CreateRequest(bool hardDelete)
        {
            //TODO -EFRAT - WIP
            var databaseCommand = new DeleteDatabaseCommand(); 

            if (hardDelete)
                databaseCommand.Url = "&hard-delete=true";
            else
                databaseCommand.Url = "&hard-delete=false";

            return databaseCommand;
        }

        public void SetResult(DeleteDatabaseResult result)
        {
            
        }
    }
}