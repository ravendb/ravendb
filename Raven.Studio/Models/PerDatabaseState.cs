using System;
using System.Collections.Generic;
using Raven.Studio.Features.Query;
using Raven.Studio.Features.Tasks;

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
			RecentAddresses = new Dictionary<string, Dictionary<string, AddressData>>();
            ActiveTasks = new Dictionary<Type, DatabaseTask>();
        }

        public DocumentViewStateStore DocumentViewState { get; private set; }

        public QueryStateStore QueryState { get; private set; }

        public QueryHistoryManager QueryHistoryManager { get; private set; }

		public Dictionary<string, Dictionary<string, AddressData>> RecentAddresses { get; set; }

        public Dictionary<Type, DatabaseTask> ActiveTasks { get; private set; }
    }
}