using Raven.Abstractions.Data;
using Raven.Client.Silverlight.Connection;

namespace Raven.Studio.Features.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.ComponentModel.Composition.Hosting;
	using System.Linq;
	using System.Net;
	using System.Windows.Browser;
	using System.Windows.Threading;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Client.Extensions;
	using Framework.Extensions;
	using Messages;
	using Newtonsoft.Json.Linq;
	using Plugins;
	using Plugins.Statistics;
	using Statistics;
	using Action = System.Action;

	[Export(typeof(IServer))]
	public class Server : PropertyChangedBase, IServer,
		IHandle<StatisticsUpdateRequested>
	{
		const string DefaultDatabaseName = "Default Database";
		readonly IEventAggregator events;

		readonly Dictionary<string, DatabaseStatistics> snapshots = new Dictionary<string, DatabaseStatistics>();

		readonly StatisticsViewModel statistics;
		readonly AggregateCatalog catalog;
		readonly DispatcherTimer timer;

		readonly TimeSpan updateFrequency = new TimeSpan(0, 0, 0, 5, 0);

		string currentDatabase;

		IEnumerable<string> databases;
		IEnumerable<ServerError> errors;

		bool isInitialized;
		string status;

		[ImportingConstructor]
		public Server(IEventAggregator events, StatisticsViewModel statistics, AggregateCatalog catalog)
		{
			this.events = events;
			this.statistics = statistics;
			this.catalog = catalog;

			timer = new DispatcherTimer { Interval = updateFrequency };
			timer.Tick += delegate { RetrieveStatisticsForCurrentDatabase(); };
			events.Subscribe(this);

			Status = "Initalizing";
			Databases = new string[] { };
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

		void IHandle<StatisticsUpdateRequested>.Handle(StatisticsUpdateRequested message) { RefreshStatistics(false); }

		public void Connect(Uri serverAddress, Action callback)
		{
			Status = "Connecting to server...";

			Address = serverAddress.OriginalString;

			Store = new DocumentStore { Url = Address };
			Store.Initialize();

			LoadPlugins();

			using (var session = Store.OpenAsyncSession())
				session.Advanced.AsyncDatabaseCommands
					.GetDatabaseNamesAsync()
					.ContinueWith(
						task =>
						{
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
							callback();
						});
		}

		void LoadPlugins()
		{
			var jsonRequestFactory = new HttpJsonRequestFactory();
			var baseUrl = (Address + "/silverlight/plugins").NoCache();
			var credentials = new NetworkCredential();
			var convention = new DocumentConvention();

			var request = jsonRequestFactory.CreateHttpJsonRequest(this, baseUrl, "GET", credentials, convention);
			var response = request.ReadResponseStringAsync();

			response.ContinueWith(_ => Execute.OnUIThread(() =>
			{
				{
					var urls = from item in JArray.Parse(_.Result)
							   let url = item.Value<string>()
							   select url;

					var catalogs = from url in urls
								   let fullUrl = Address + "/silverlight/plugin" + url.Replace('\\', '/')
								   let uri = new Uri(fullUrl, UriKind.Absolute)
								   select new DeploymentCatalog(uri);

					foreach (var deployment in catalogs)
					{
						deployment.DownloadCompleted += (s, e) =>
						{
							if (e.Error != null)
								throw e.Error;
						};
						deployment.DownloadAsync();
						catalog.Catalogs.Add(deployment);
					}
				}
			}));
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
										databases = databases.Union(new[] { databaseName });
										NotifyOfPropertyChange(() => Databases);
										CurrentDatabase = databaseName;
									});
		}

		public string Address { get; private set; }
		public string CurrentDatabaseAddress
		{
			get
			{
				return (CurrentDatabase == "Default Database")
					? Address
					: Address + "/databases/" + HttpUtility.UrlEncode(CurrentDatabase);
			}
		}

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
			events.Publish(new StatisticsUpdated(mostRecent) { HasDocumentCountChanged = docsChanged });
		}
	}
}