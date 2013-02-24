using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class DatabaseListItemModel : Model
    {
        private readonly string name;
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;

        public DatabaseListItemModel(string name)
        {
            this.name = name;
            Statistics = new Observable<DatabaseStatistics>();

            var documentStore = ApplicationModel.Current.Server.Value.DocumentStore;

            asyncDatabaseCommands = name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase)
								? documentStore.AsyncDatabaseCommands.ForSystemDatabase()
                                : documentStore.AsyncDatabaseCommands.ForDatabase(name);
        }

        public Observable<DatabaseStatistics> Statistics { get; set; }

        public string Name
        {
            get { return name; }
        }

        public override Task TimerTickedAsync()
        {
            return RefreshStatistics();
        }

        private Task RefreshStatistics()
        {
            return asyncDatabaseCommands
                .GetStatisticsAsync()
                .ContinueOnSuccessInTheUIThread(s => Statistics.Value = s);
        }
    }
}