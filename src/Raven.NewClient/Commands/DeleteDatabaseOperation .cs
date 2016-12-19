using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class DeleteDatabaseOperation
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<DeleteDatabaseOperation>("Raven.NewClient.Client");

        private string _databaseName;

        public DeleteDatabaseOperation()
        {
        }

        protected void LogBatch()
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Database '{_databaseName}' deleted");
        }

        public DeleteDatabaseCommand CreateRequest(string name, bool hardDelete)
        {
            _databaseName = name;
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