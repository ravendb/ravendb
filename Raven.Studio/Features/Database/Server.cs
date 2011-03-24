namespace Raven.Studio.Features.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using System.Windows.Threading;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Client.Extensions;
	using Framework;
	using Messages;
	using Raven.Database.Data;
	using StartUp;
	using Statistics;
	using Action = System.Action;

	[Export(typeof (IServer))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class Server : PropertyChangedBase, IServer, IHandle<StatisticsUpdateRequested>
	{
		const string DefaultDatabaseName = "Default Database";
		readonly IEventAggregator events;
		readonly IDatabaseInitializer[] databaseInitializers;

		readonly Dictionary<string, DatabaseStatistics> snapshots = new Dictionary<string, DatabaseStatistics>();

		readonly List<string> startupChecks = new List<string>();
		readonly StatisticsViewModel statistics;
		readonly DispatcherTimer timer;

		readonly TimeSpan updateFrequency = new TimeSpan(0, 0, 0, 5, 0);

		string currentDatabase;

		IEnumerable<string> databases;
		IEnumerable<ServerError> errors;

		bool isInitialized;

		[ImportingConstructor]
		public Server(IEventAggregator events, [ImportMany] IDatabaseInitializer[] databaseInitializers, StatisticsViewModel statistics)
		{
			this.events = events;
			this.databaseInitializers = databaseInitializers;
			this.statistics = statistics;

			timer = new DispatcherTimer {Interval = updateFrequency};
			timer.Tick += delegate { RetrieveStatisticsForCurrentDatabase(); };
			events.Subscribe(this);
		}

		public bool HasCurrentDatabase { get { return !string.IsNullOrEmpty(CurrentDatabase); } }
		public void Handle(StatisticsUpdateRequested message) { RefreshStatistics(false); }
        public IDocumentStore Store { get; private set; }

		public void Connect(Uri serverAddress, Action callback)
		{
			Address = serverAddress.OriginalString;
			Name = serverAddress.OriginalString;

            Store = new DocumentStore { Url = Address };
            Store.Initialize();

			SelectDatabase(DefaultDatabaseName, callback);

            Store.OpenAsyncSession().Advanced.AsyncDatabaseCommands
				.GetDatabaseNamesAsync()
				.ContinueOnSuccess(t =>
				                   	{
				                   		var dbs = new List<string>
				                   		          	{
				                   		          		DefaultDatabaseName
				                   		          	};
				                   		dbs.AddRange(t.Result);
				                   		Databases = dbs;

				                   		SelectDatabase(dbs[0], callback);
				                   	});
		}

		public string CurrentDatabase { get { return currentDatabase; } }

		public void OpenDatabase(string name, Action callback)
		{
			if (callback == null) callback = () => { };

			if (name == currentDatabase)
			{
				callback();
				return;
			}

			currentDatabase = name;
			InitializeCurrentDatabase(() =>
			                          	{
			                          		NotifyOfPropertyChange(() => CurrentDatabase);
			                          		NotifyOfPropertyChange(() => HasCurrentDatabase);
			                          		RefreshStatistics(true);
			                          		RaiseCurrentDatabaseChanged();

			                          		callback();
			                          	});
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
                .ContinueWith(create =>
                                  {
                                     if(callback != null) callback();
                                      databases = databases.Union(new[] {databaseName});
                                      NotifyOfPropertyChange(()=>Databases);
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
		public event EventHandler Connected = delegate { };

		public IEnumerable<ServerError> Errors
		{
			get { return errors; }
			private set
			{
				errors = value;
				NotifyOfPropertyChange(() => Errors);
			}
		}

		void SelectDatabase(string name, Action callback)
		{
			OpenDatabase(name, () =>
			                   	{
			                   		IsInitialized = true;
			                   		Execute.OnUIThread(() => timer.Start());

			                   		Connected(this, EventArgs.Empty);

			                   		if (callback != null) callback();
			                   	});
		}

		void RaiseCurrentDatabaseChanged() { CurrentDatabaseChanged(this, EventArgs.Empty); }

		void InitializeCurrentDatabase(Action callback)
		{
			if (startupChecks.Contains(CurrentDatabase)) return;
			startupChecks.Add(CurrentDatabase);

			using (var session = OpenSession())
			{
				var tasks = from initializer in databaseInitializers
				            from task in initializer.Initialize(session)
				            select task;
				tasks.ExecuteInSequence(callback);
			}
		}

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
					.ContinueOnSuccess(x =>
					                       {
					                           snapshots[CurrentDatabase] = x.Result;
					                           ProcessStatistics(x.Result);
					                       });
			}
		}

	    private void ProcessStatistics(DatabaseStatistics mostRecent)
	    {
	        statistics.Accept(mostRecent);
	        Errors = mostRecent.Errors.OrderByDescending(error => error.Timestamp);
	        events.Publish(new StatisticsUpdated(mostRecent));
	    }
	}
}