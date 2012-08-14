using System;
using System.Collections.ObjectModel;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Data;
using Raven.Client.Changes;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Silverlight.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using System.Linq;
using System.Reactive.Linq;
using Raven.Studio.Extensions;

namespace Raven.Studio.Models
{
	public class DatabaseModel : Model, IDisposable
	{
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private readonly string name;
		private readonly DocumentStore documentStore;

		private IObservable<DocumentChangeNotification> documentChanges;
		private IObservable<IndexChangeNotification> indexChanges;

		private readonly CompositeDisposable disposable = new CompositeDisposable();

		public Observable<TaskModel> SelectedTask { get; set; }
		public Observable<DatabaseDocument> DatabaseDocument { get; set; }

		public DatabaseModel(string name, DocumentStore documentStore)
		{
			this.name = name;
			this.documentStore = documentStore;
			ReplicationOnline = new ObservableCollection<string>();

			Tasks = new BindableCollection<TaskModel>(x => x.Name)
			{
				new ImportTask(),
				new ExportTask(),
				new StartBackupTask(),
				new IndexingTask()
			};

			SelectedTask = new Observable<TaskModel> { Value = Tasks.FirstOrDefault() };
			Statistics = new Observable<DatabaseStatistics>();
			Status = new Observable<string>
			{
				Value = "Offline"
			};

			asyncDatabaseCommands = name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase)
											? documentStore.AsyncDatabaseCommands.ForDefaultDatabase()
											: documentStore.AsyncDatabaseCommands.ForDatabase(name);

			DocumentChanges.Select(c => Unit.Default).Merge(IndexChanges.Select(c => Unit.Default))
				.SampleResponsive(TimeSpan.FromSeconds(2))
				.Subscribe(_ => RefreshStatistics());

			RefreshStatistics();
		}

		public void UpdateDatabaseDocument()
		{
			if (ApplicationModel.Current != null)
				ApplicationModel.Current.Server.Value.DocumentStore
					.AsyncDatabaseCommands
					.ForDefaultDatabase()
					.GetAsync("Raven/Databases/" + Name)
					.ContinueOnSuccessInTheUIThread(doc =>
					{
						if (doc == null)
							return;
						DatabaseDocument = new Observable<DatabaseDocument>
						{
							Value = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer().Deserialize
								<DatabaseDocument>(new RavenJTokenReader(doc.DataAsJson))
						};
						OnPropertyChanged(() => HasReplication);
						UpdateReplicationOnlineStatus();
					});
		}

		public void UpdateReplicationOnlineStatus()
		{
			if (HasReplication == false)
				return;
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
						asyncServerClient.CreateRequest("GET", "/replication/info")
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

		public bool HasReplication
		{
			get
			{
				return DatabaseDocument != null &&
					   DatabaseDocument.Value.Settings["Raven/ActiveBundles"].Contains("Replication");
			}
		}

		public BindableCollection<TaskModel> Tasks { get; private set; }

		public IObservable<DocumentChangeNotification> DocumentChanges
		{
			get
			{
				if (documentChanges == null)
				{
					documentChanges = Changes()
						.ForAllDocuments()
						.Publish(); // use a single underlying subscription

					var documentChangesSubscription =
						((IConnectableObservable<DocumentChangeNotification>)documentChanges).Connect();

					disposable.Add(documentChangesSubscription);
				}

				return documentChanges;
			}
		}

		public IObservable<IndexChangeNotification> IndexChanges
		{
			get
			{
				if (indexChanges == null)
				{
					indexChanges = Changes()
						.ForAllIndexes()
						.Publish(); // use a single underlying subscription

					var indexChangesSubscription = ((IConnectableObservable<IndexChangeNotification>)indexChanges).Connect();
					disposable.Add(indexChangesSubscription);
				}

				return indexChanges;
			}
		}

		public IDatabaseChanges Changes()
		{
			return name == Constants.SystemDatabase ?
				documentStore.Changes() :
				documentStore.Changes(name);
		}

		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return asyncDatabaseCommands; }
		}

		public string Name
		{
			get { return name; }
		}

		public Observable<DatabaseStatistics> Statistics { get; set; }

		public Observable<string> Status { get; set; }

		private void RefreshStatistics()
		{
			asyncDatabaseCommands
				.GetStatisticsAsync()
				.ContinueOnSuccess(stats =>
				{
					Statistics.Value = stats;
					Status.Value = "Online";
				})
				.Catch(exception => Status.Value = "Offline");
		}

		public void Dispose()
		{
			disposable.Dispose();
		}
	}
}
