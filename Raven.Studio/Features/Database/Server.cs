namespace Raven.Studio.Features.Database
{
	using System;
	using System.Collections;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using System.Windows.Threading;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Framework;
	using Messages;
	using Plugin;
	using Raven.Database.Data;
	using StartUp;
	using Action = System.Action;

	[Export(typeof(IServer))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class Server : PropertyChangedBase, IServer,
						  IHandle<StatisticsUpdateRequested>
	{
		const string DefaultDatabaseName = "Default Database";
		readonly IDatabaseInitializer[] databaseInitializers;

		readonly Dictionary<string, DatabaseStatistics> snapshots
			= new Dictionary<string, DatabaseStatistics>();

		readonly List<string> startupChecks = new List<string>();
		readonly DispatcherTimer timer;

		readonly TimeSpan updateFrequency = new TimeSpan(0, 0, 0,
														 5, 0);

		string currentDatabase;

		IEnumerable<string> databases;

		bool isInitialized;
		DatabaseStatistics statistics;
		DocumentStore store;

		[ImportingConstructor]
		public Server(IEventAggregator events,
					  [ImportMany] IDatabaseInitializer[]
						databaseInitializers)
		{
			this.databaseInitializers = databaseInitializers;

			timer = new DispatcherTimer { Interval = updateFrequency };
			timer.Tick +=
				delegate { RetrieveStatisticsForCurrentDatabase(); };
			events.Subscribe(this);
		}

		public bool HasCurrentDatabase { get { return !string.IsNullOrEmpty(CurrentDatabase); } }
		public void Handle(StatisticsUpdateRequested message) { RefreshStatistics(false); }

		public void Connect(Uri serverAddress, Action callback)
		{
			Address = serverAddress.OriginalString;
			Name = serverAddress.OriginalString;

			store = new DocumentStore { Url = Address };
			store.Initialize();

			store.OpenAsyncSession().Advanced.AsyncDatabaseCommands
				.GetDatabaseNamesAsync()
				.ContinueOnSuccess(t =>
									{
										var databases = new List<string>
				                   		                	{
				                   		                		DefaultDatabaseName
				                   		                	};
										databases.AddRange(t.Result);
										Databases = databases;

										OpenDatabase(databases[0], () =>
																	{
																		IsInitialized = true;
																		Execute.OnUIThread(() => timer.Start());

																		Connected(this, EventArgs.Empty);

																		if (callback != null) callback();
																	});

									});
		}

		public string CurrentDatabase
		{
			get { return currentDatabase; }
		}

		public void OpenDatabase(string name, Action callback)
		{
			if (name == currentDatabase) return;

			currentDatabase = name;
			InitializeCurrentDatabase(() =>
			{
				NotifyOfPropertyChange(() => CurrentDatabase);
				NotifyOfPropertyChange(() => HasCurrentDatabase);
				RefreshStatistics(true);
				RaiseCurrentDatabaseChanged();

				if (callback != null) callback();
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
					? store.OpenAsyncSession()
					: store.OpenAsyncSession(CurrentDatabase);
		}

		public DatabaseStatistics Statistics
		{
			get { return statistics; }
			private set
			{
				statistics = value;
				NotifyOfPropertyChange(() => Statistics);
			}
		}

		public event EventHandler CurrentDatabaseChanged =
			delegate { };

		public event EventHandler Connected = delegate { };

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
				ExecuteTasks(tasks, callback);
			}
		}

		void RefreshStatistics(bool clear)
		{
			if (clear) Statistics = null;

			if (snapshots.ContainsKey(CurrentDatabase))
			{
				var snapshot = snapshots[CurrentDatabase];
				Statistics = snapshot;
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
						Statistics = x.Result;
					});
			}
		}

		public void ExecuteTasks(IEnumerable<Task> tasks, Action callback)
		{
			var enumerator = tasks.GetEnumerator();
			ExecuteNextTask(enumerator, callback);
		}

		static void ExecuteNextTask(IEnumerator<Task> enumerator, Action callback)
		{
			bool moveNextSucceeded = enumerator.MoveNext();

			if (!moveNextSucceeded)
			{
				if (callback != null) callback();
				return;
			}

			enumerator
				.Current
				.ContinueWith(x => ExecuteNextTask(enumerator, callback));
		}
	}
}