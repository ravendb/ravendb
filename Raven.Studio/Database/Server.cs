namespace Raven.Studio.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Windows.Threading;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Framework;
	using Plugin;
	using Raven.Database.Data;

	public class Server : PropertyChangedBase, IServer
	{
		readonly DocumentStore store;
		bool isInitialized;
		readonly DispatcherTimer timer;
		readonly Dictionary<string, DatabaseStatistics> snapshots = new Dictionary<string, DatabaseStatistics>();
		readonly TimeSpan updateFrequency = new TimeSpan(0,0,0,5,0);

		public Server(string address, string name = null)
		{
			Address = address;
			Name = name ?? address;

			store = new DocumentStore {Url = Address};
			store.Initialize();

			timer = new DispatcherTimer { Interval = updateFrequency };
			timer.Tick += delegate{RetrieveStatisticsForCurrentDatabase();};

			store.OpenAsyncSession().Advanced.AsyncDatabaseCommands
				.GetDatabaseNamesAsync()
				.ContinueOnSuccess(t => 
				{
					var databases = new List<string>{"Default"};
					databases.AddRange(t.Result);
					Databases = databases;

					CurrentDatabase = databases[0];

					IsInitialized = true;
					Execute.OnUIThread( ()=>timer.Start() );
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
			}
		}

		[ImportMany(AllowRecomposition = true)]
		public IList<IPlugin> Plugins { get; set; }

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
			return (CurrentDatabase == "Default") 
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
	}
}