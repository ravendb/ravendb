using System;
using System.Collections.ObjectModel;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Data;
using Raven.Client;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Silverlight.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class ReplicationStatisticsModel : ViewModel
    {
        public ReplicationStatisticsModel()
        {
            ReplicationOnline = new ObservableCollection<string>();
            Name = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

            documentStore = ApplicationModel.Current.Server.Value.DocumentStore;
            asyncDatabaseCommands = Name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase)
                                            ? documentStore.AsyncDatabaseCommands.ForDefaultDatabase()
                                            : documentStore.AsyncDatabaseCommands.ForDatabase(Name);
        }

        public void UpdateReplicationOnlineStatus()
        {
            ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(Name)
                .LoadAsync<ReplicationDocument>("Raven/Replication/Destinations")
                .ContinueOnSuccessInTheUIThread(document =>
                {
                    ReplicationOnline = new ObservableCollection<string>();
                    var asyncServerClient = asyncDatabaseCommands as AsyncServerClient;
                    if (asyncServerClient == null)
                        return;

                    if (document == null)
                        return;

                    foreach (var replicationDestination in document.Destinations)
                    {
                        var destination = replicationDestination;
                        asyncServerClient.CreateRequest("/replication/info", "GET")
                            .ReadResponseJsonAsync()
                            .ContinueWith(task =>
                            {
                                if (task.IsFaulted)
                                    throw new InvalidOperationException("Could not get replication info");

                                var url = task.Result.SelectToken("Self").ToString();
                                var lastEtag = task.Result.SelectToken("MostRecentDocumentEtag").ToString();

                                if (string.IsNullOrEmpty(url) || string.IsNullOrEmpty(lastEtag))
                                    throw new Exception("Replication info is not as expected");

                                asyncServerClient.DirectGetAsync(destination.Url + "/databases/" + destination.Database, "Raven/Replication/Sources/" + url).
                                    ContinueOnSuccessInTheUIThread(data =>
                                    {
                                        if (data == null)
                                        {
                                            ReplicationOnline.Add(destination.Url + " - Offline");
                                            OnPropertyChanged(() => ReplicationOnline);
                                            return;
                                        }

                                        var sourceReplicationInformation = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.
                                            CreateSerializer().Deserialize
                                            <SourceReplicationInformation>(new RavenJTokenReader(data.DataAsJson));
                                        if (sourceReplicationInformation == null)
                                            ReplicationOnline.Add(destination.Url + " - Offline");
                                        else
                                        {

                                            if (lastEtag == sourceReplicationInformation.LastDocumentEtag.ToString())
                                                ReplicationOnline.Add(destination.Url + " - Updated");
                                            else
                                                ReplicationOnline.Add(destination.Url + " - Online");

                                        }
                                        OnPropertyChanged(() => ReplicationOnline);
                                    })
                                    .Catch(_ =>
                                    {
                                        ReplicationOnline.Add(destination.Url + " - Offline");
                                        OnPropertyChanged(() => ReplicationOnline);
                                    });
                            });
                    }
                }).Catch();
        }

        public ObservableCollection<string> ReplicationOnline { get; set; }
        public string Name { get; set; }
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
        private readonly IDocumentStore documentStore;
    }
}