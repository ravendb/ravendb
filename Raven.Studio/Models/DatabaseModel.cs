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
	    private IDatabaseChanges databaseChanges;

		public Observable<TaskModel> SelectedTask { get; set; }
		public Observable<DatabaseDocument> DatabaseDocument { get; set; }

		public DatabaseModel(string name, DocumentStore documentStore)
		{
			this.name = name;
			this.documentStore = documentStore;

			Tasks = new BindableCollection<TaskModel>(x => x.Name)
			{
				new ImportTask(),
				new ExportTask(),
				new StartBackupTask(),
				new StartRestoreTask(),
				new IndexingTask(),
				new SampleDataTask(),
                new CsvImportTask()
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
		        .Subscribe(_ => RefreshStatistics(), exception => ApplicationModel.Current.Server.Value.IsConnected.Value = false);

			databaseChanges.ConnectionStatusCahnged += (sender, args) =>
			{
				ApplicationModel.Current.Server.Value.IsConnected.Value = (sender as IDatabaseChanges).Connected;
				OnPropertyChanged(() => ApplicationModel.Current.Server.Value.IsConnected);
				UpdateStatus();
			};

			RefreshStatistics();
		}

		private void UpdateStatus()
		{
			Status.Value = ApplicationModel.Current.Server.Value.IsConnected.Value ? "Online" : "Offline";
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
					});
		}

		public bool HasReplication
		{
			get
			{
				return DatabaseDocument != null && 
					   DatabaseDocument.Value.Settings != null && 
					   DatabaseDocument.Value.Settings.ContainsKey("Raven/ActiveBundles") &&
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
					var changes = Changes();

					ApplicationModel.ChangesToDispose.Add(changes);
					documentChanges = changes
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
					var changes = Changes();
					ApplicationModel.ChangesToDispose.Add(changes);
					indexChanges = changes
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
            return databaseChanges ??
                   (databaseChanges =
                    name == Constants.SystemDatabase
                        ? documentStore.Changes()
                        : documentStore.Changes(name));
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
				});
		}

	    public void Dispose()
		{
	        disposable.Dispose();

            using ((IDisposable)databaseChanges)
		{
		}
		}
	}
}
