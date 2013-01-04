using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection.Async;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class ReplicationStatisticsModel : ViewModel
    {
        public ReplicationStatisticsModel()
        {
			Stats = new List<DestinationStats>();
            Name = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

            documentStore = ApplicationModel.Current.Server.Value.DocumentStore;
            asyncDatabaseCommands = Name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase)
                                            ? documentStore.AsyncDatabaseCommands.ForDefaultDatabase()
                                            : documentStore.AsyncDatabaseCommands.ForDatabase(Name);

			UpdateReplicationOnlineStatus();
        }

		public void UpdateReplicationOnlineStatus()
		{
			var asyncServerClient = asyncDatabaseCommands as AsyncServerClient;
			if (asyncServerClient == null)
				return;

			asyncServerClient.CreateRequest("/replication/info?noCache="+Guid.NewGuid(), "GET")
					.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						if (task.IsFaulted)
							throw new InvalidOperationException("Could not get replication info");

						var replicationStats = new JsonSerializer().Deserialize<ReplicationStatistics>(new RavenJTokenReader(task.Result));

						if (replicationStats == null)
							throw new Exception("Replication info is not as expected");

						Stats = replicationStats.Stats;
						OnPropertyChanged(() => Stats);
					});
		}

		public override System.Threading.Tasks.Task TimerTickedAsync()
		{
			UpdateReplicationOnlineStatus();
			return base.TimerTickedAsync();
		}

	    public string Name { get; set; }
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
        private readonly IDocumentStore documentStore;
		public List<DestinationStats> Stats { get; set; } 
    }
}