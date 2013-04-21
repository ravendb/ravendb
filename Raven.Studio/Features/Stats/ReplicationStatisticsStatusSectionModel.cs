using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Stats
{
    public class ReplicationStatisticsStatusSectionModel : StatusSectionModel
    {
        public ReplicationStatisticsStatusSectionModel()
        {
	        SectionName = "Replication Statistics";
			Stats = new List<DestinationStats>();
            Name = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

            documentStore = ApplicationModel.Current.Server.Value.DocumentStore;
            asyncDatabaseCommands = Name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase)
											? documentStore.AsyncDatabaseCommands.ForSystemDatabase()
                                            : documentStore.AsyncDatabaseCommands.ForDatabase(Name);

			UpdateReplicationOnlineStatus();
        }

	    public void UpdateReplicationOnlineStatus()
	    {
		    var asyncServerClient = asyncDatabaseCommands as AsyncServerClient;
		    if (asyncServerClient == null)
			    return;

		    asyncServerClient.Info.GetReplicationInfoAsync()
		                     .ContinueWith(task =>
		                     {
			                     if (task.IsFaulted)
				                     throw new InvalidOperationException("Could not get replication info");

			                     var replicationStats = task.Result;
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