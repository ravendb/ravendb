namespace Raven.Studio.Features.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Windows.Threading;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Client.Extensions;
	using Framework;
	using Messages;
	using Raven.Database.Data;
	using Statistics;
	using Action = System.Action;

	[Export(typeof (IServer))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class Server : PropertyChangedBase, IServer, IHandle<StatisticsUpdateRequested>
	{
		const string DefaultDatabaseName = "Default Database";
		readonly IEventAggregator events;

		readonly Dictionary<string, DatabaseStatistics> snapshots = new Dictionary<string, DatabaseStatistics>();

		readonly StatisticsViewModel statistics;
		readonly DispatcherTimer timer;

		readonly TimeSpan updateFrequency = new TimeSpan(0, 0, 0, 5, 0);

		string currentDatabase;

		IEnumerable<string> databases;
		IEnumerable<ServerError> errors;

		bool isInitialized;
		string status;

		[ImportingConstructor]
		public Server(IEventAggregator events, StatisticsViewModel statistics)
		{
			this.events = events;
			this.statistics = statistics;

			timer = new DispatcherTimer {Interval = updateFrequency};
			timer.Tick += delegate { RetrieveStatisticsForCurrentDatabase(); };
			events.Subscribe(this);

			Status = "Initalizing";
			Databases = new string[]{};
		}

		public bool HasCurrentDatabase { get { return !string.IsNullOrEmpty(CurrentDatabase); } }
		public IDocumentStore Store { get; private set; }

		public string Status
		{
			get { return status; }
			set
			{
				status = value;
				NotifyOfPropertyChange(() => Status);
			}
		}

		public void Handle(StatisticsUpdateRequested message) { RefreshStatistics(false); }

		public void Connect(Uri serverAddress, Action callback)
		{
			Status = "Connecting to server...";

			Address = serverAddress.OriginalString;
			Name = serverAddress.OriginalString;

			Store = new DocumentStore {Url = Address};
			Store.Initialize();

			Store.OpenAsyncSession().Advanced.AsyncDatabaseCommands
				.GetDatabaseNamesAsync()
				.ContinueWith(
					task =>
						{
							IsInitialized = true;
							Status = "Connected";
							var dbs = new List<string>
							          	{
							          		DefaultDatabaseName
							          	};
							dbs.AddRange(task.Result);
							Databases = dbs;

							OpenDatabase(dbs[0], () =>
							{
								Execute.OnUIThread(() => { if (!timer.IsEnabled) timer.Start(); });

								if (callback != null) callback();
							});
						},
					faulted =>
						{
							var error = "Unable to connect to " + Address;
							Status = error;
							events.Publish(new NotificationRaised(error, NotificationLevel.Error));
							IsInitialized = false;
							callback();
						});
		}

		public string CurrentDatabase
		{
			get { return currentDatabase; }
			set
			{
				if (value == currentDatabase) return;

				currentDatabase = value;
				NotifyOfPropertyChange(() => CurrentDatabase);
				NotifyOfPropertyChange(() => HasCurrentDatabase);
			}
		}

		public void OpenDatabase(string name, Action callback)
		{
			if (callback == null) callback = () => { };

			CurrentDatabase = name;
			RefreshStatistics(true);
			RaiseCurrentDatabaseChanged();

			using (var session = OpenSession())
				session.Advanced.AsyncDatabaseCommands.EnsureSilverlightStartUpAsync();

			callback();
		}

		public IEnumerable<string> Databases
		{
			get { return databases; }
			private set
			{
				databases = value;
				NotifyOfPropertyChange(() => Databases);
			}
		}

		public void CreateDatabase(string databaseName, Action callback)
		{
			Store.AsyncDatabaseCommands
				.EnsureDatabaseExistsAsync(databaseName)
				.ContinueWith(task =>
				              	{
				              		if (task.Exception != null)
				              			return task;

				              		return Store.AsyncDatabaseCommands
				              			.ForDatabase(databaseName)
				              			.EnsureSilverlightStartUpAsync();
				              	})
				.ContinueOnSuccess(create =>
				                   	{
				                   		if (callback != null) callback();
				                   		databases = databases.Union(new[] {databaseName});
				                   		NotifyOfPropertyChange(() => Databases);
				                   	});
		}

		public bool IsInitialized
		{
			get { return isInitialized; }
			private set
			{
				isInitialized = value;
				NotifyOfPropertyChange(() => IsInitialized);
			}
		}

		public string Address { get; private set; }
		public string Name { get; private set; }

		public IAsyncDocumentSession OpenSession()
		{
			return (CurrentDatabase == DefaultDatabaseName)
			       	? Store.OpenAsyncSession()
			       	: Store.OpenAsyncSession(CurrentDatabase);
		}

		public IStatisticsSet Statistics { get { return statistics; } }

		public event EventHandler CurrentDatabaseChanged = delegate { };

		public IEnumerable<ServerError> Errors
		{
			get { return errors; }
			private set
			{
				errors = value;
				NotifyOfPropertyChange(() => Errors);
			}
		}

		void RaiseCurrentDatabaseChanged() { CurrentDatabaseChanged(this, EventArgs.Empty); }

		void RefreshStatistics(bool clear)
		{
			//if (clear) statistics = new StatisticsViewModel();

			if (snapshots.ContainsKey(CurrentDatabase))
			{
				ProcessStatistics(snapshots[CurrentDatabase]);
			}

			RetrieveStatisticsForCurrentDatabase();
		}

		void RetrieveStatisticsForCurrentDatabase()
		{
			if (!HasCurrentDatabase) return;

			using (var session = OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetStatisticsAsync()
					.ContinueOnSuccess(x => ProcessStatistics(x.Result));
			}
		}

		void ProcessStatistics(DatabaseStatistics mostRecent)
		{
			bool docsChanged = false;
			if (snapshots.ContainsKey(CurrentDatabase))
			{
				docsChanged = (snapshots[CurrentDatabase].CountOfDocuments != mostRecent.CountOfDocuments);
			}

			snapshots[CurrentDatabase] = mostRecent;
			statistics.Accept(mostRecent);
			Errors = mostRecent.Errors.OrderByDescending(error => error.Timestamp);
			events.Publish(new StatisticsUpdated(mostRecent) {HasDocumentCountChanged = docsChanged});
		}
	}
}