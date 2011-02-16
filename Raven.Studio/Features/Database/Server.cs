namespace Raven.Studio.Features.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Windows.Threading;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Framework;
	using Messages;
	using Plugin;
	using Raven.Database.Data;
	using Action = System.Action;

    [Export(typeof(IServer))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class Server : PropertyChangedBase, IServer, IHandle<StatisticsUpdateRequested>
	{
		const string DefaultDatabaseName = "Default Database";
		DocumentStore store;
		bool isInitialized;
		readonly DispatcherTimer timer;
		readonly Dictionary<string, DatabaseStatistics> snapshots = new Dictionary<string, DatabaseStatistics>();
		readonly TimeSpan updateFrequency = new TimeSpan(0,0,0,5,0);
		
		[ImportingConstructor]
		public Server(IEventAggregator events)
		{
			//TODO: Do we really need a timer? Only if we want to see update from other users working on the same database
			timer = new DispatcherTimer { Interval = updateFrequency };
			timer.Tick += delegate { RetrieveStatisticsForCurrentDatabase(); };
			events.Subscribe(this);
		}

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
					var databases = new List<string> { DefaultDatabaseName };
					databases.AddRange(t.Result);
					Databases = databases;

					CurrentDatabase = databases[0];

					IsInitialized = true;
					//Execute.OnUIThread(() => timer.Start());

                    if (callback != null) callback();
				});
		}

		string currentDatabase;
		public string CurrentDatabase
		{
			get { return currentDatabase; }
			set
			{
				currentDatabase = value; 
				NotifyOfPropertyChange( ()=> CurrentDatabase);
				NotifyOfPropertyChange(() => HasCurrentDatabase);
				RefreshStatistics(true);
				RaiseCurrentDatabaseChanged();
			}
		}

		IEnumerable<string> databases;
		public IEnumerable<string> Databases
		{
			get { return databases; }
			private set { databases = value; NotifyOfPropertyChange( ()=> Databases); }
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

		public bool HasCurrentDatabase
		{
			get {return !string.IsNullOrEmpty(CurrentDatabase); }
		}

		DatabaseStatistics statistics;
		public DatabaseStatistics Statistics
		{
			get { return statistics; }
			private set
			{
				statistics = value;
				NotifyOfPropertyChange(() => Statistics);
			}
		}

		public event EventHandler CurrentDatabaseChanged = delegate { };
		void RaiseCurrentDatabaseChanged()
		{
			CurrentDatabaseChanged(this,EventArgs.Empty);
		}

		void RefreshStatistics(bool clear)
		{
			if(clear) Statistics = null;

			if(snapshots.ContainsKey(CurrentDatabase))
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

		public void Handle(StatisticsUpdateRequested message)
		{
			RefreshStatistics(false);
		}
	}
}