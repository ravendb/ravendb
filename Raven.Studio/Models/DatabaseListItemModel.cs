using System;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
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
                                ? documentStore.AsyncDatabaseCommands.ForDefaultDatabase()
                                : documentStore.AsyncDatabaseCommands.ForDatabase(name);
        }

        public Observable<DatabaseStatistics> Statistics { get; set; }

        public string Name
        {
            get { return name; }
        }

        public override System.Threading.Tasks.Task TimerTickedAsync()
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
