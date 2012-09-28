using Raven.Studio.Features.Query;

namespace Raven.Studio.Models
{
    public class PerDatabaseState
    {
        public string DatabaseName { get; private set; }

        public PerDatabaseState(string databaseName)
        {
            DatabaseName = databaseName;
            DocumentViewState = new DocumentViewStateStore();
            QueryState = new QueryStateStore();
            QueryHistoryManager = new QueryHistoryManager(DatabaseName);
        }

        public DocumentViewStateStore DocumentViewState { get; private set; }

        public QueryStateStore QueryState { get; private set; }

        public QueryHistoryManager QueryHistoryManager { get; private set; }
    }
}