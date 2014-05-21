using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.RavenFS.Connections;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using TaskEx = System.Threading.Tasks.Task;

namespace Raven.Client.RavenFS.Changes
{
	public class ServerNotifications : IServerNotifications, IObserver<string>, IDisposable, IHoldProfilingInformation
	{
		private readonly string url;
		private readonly FileConvention convention;
		private readonly AtomicDictionary<NotificationSubject> subjects = new AtomicDictionary<NotificationSubject>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<Task> pendingConnectionTasks = new ConcurrentSet<Task>();
		private int reconnectAttemptsRemaining;
		private IDisposable connection;
		private HttpJsonRequestFactory jsonRequestFactory =
#if !NETFX_CORE
 new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  new HttpJsonRequestFactory();
#endif
		private const int DefaultNumberOfCachedRequests = 2048;

		private static int connectionCounter;
		private readonly string id;
		private Task connectionTask;
		private object gate = new object();

		public ServerNotifications(string url, FileConvention convention)
		{
			id = Interlocked.Increment(ref connectionCounter) + "/" +
				 Base62Util.Base62Random();
			this.url = url;
			this.convention = convention;
		}

		private async Task EstablishConnection()
		{
			//TODO: Fix not to use WebRequest
			var request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url + "/changes/events?id=" + id,
					"GET", new OperationCredentials("", new CredentialCache()), convention)
				{
					DisableRequestCompression = true,
					AvoidCachingRequest = true
				})
				;

			while (true)
			{
				try
				{
					var result = await request.ServerPullAsync();
					reconnectAttemptsRemaining = 3; // after the first successful try, we will retry 3 times before giving up
					connection = (IDisposable) result;
					result.Subscribe(this);

					return;
				}
				catch (Exception)
				{
					if (reconnectAttemptsRemaining <= 0)
						throw;

					reconnectAttemptsRemaining--;
				}
			}
		}

		public Task ConnectionTask
		{
			get
			{
				EnsureConnectionInitiated();
				return connectionTask;
			}
		}

		private void EnsureConnectionInitiated()
		{
			if (connectionTask != null)
				return;

			lock (gate)
			{
				if (connectionTask != null)
					return;

				connectionTask = EstablishConnection()
				.ObserveException();
			}
		}

		private async Task AfterConnection(Func<Task> action)
		{
			try
			{
				await ConnectionTask;
				await action();
			}
			catch (Exception)
			{

			}
		}

		public Task WhenSubscriptionsActive()
		{
			return TaskEx.WhenAll(pendingConnectionTasks);
		}

		public IObservable<ConfigurationChangeNotification> ConfigurationChanges()
		{
			EnsureConnectionInitiated();

#pragma warning disable 4014
			var observable = subjects.GetOrAdd("config", s => new NotificationSubject<ConfigurationChangeNotification>(
                                                               () => ConfigureConnection("watch-config"),
															   () => ConfigureConnection("unwatch-config"),
															   item => true));
#pragma warning restore 4014

			return (IObservable<ConfigurationChangeNotification>)observable;
		}

		private async Task ConfigureConnection(string command, string value = "")
		{
			var afterConnection = AfterConnection(() => Send(command, value));

			pendingConnectionTasks.Add(afterConnection);
			await afterConnection;
			pendingConnectionTasks.TryRemove(afterConnection);
		}

		public IObservable<ConflictNotification> Conflicts()
		{
			EnsureConnectionInitiated();

#pragma warning disable 4014
			var observable = subjects.GetOrAdd("conflicts", s => new NotificationSubject<ConflictNotification>(
                                                               () => ConfigureConnection("watch-conflicts"),
															   () => ConfigureConnection("unwatch-conflicts"),
															   item => true));
#pragma warning restore 4014

			return (IObservable<ConflictNotification>)observable;
		}

		public IObservable<FileChangeNotification> FolderChanges(string folder)
		{
			if (!folder.StartsWith("/"))
			{
				throw new ArgumentException("folder must start with /");
			}

			var canonicalisedFolder = folder.TrimStart('/');

			EnsureConnectionInitiated();

#pragma warning disable 4014
			var observable = subjects.GetOrAdd("folder/" + canonicalisedFolder, s => new NotificationSubject<FileChangeNotification>(
                                                               () => ConfigureConnection("watch-folder", folder),
															   () => ConfigureConnection("unwatch-folder", folder),
															   f => f.File.StartsWith(folder, StringComparison.InvariantCultureIgnoreCase)));
#pragma warning restore 4014

			return (IObservable<FileChangeNotification>)observable;
		}

		public IObservable<SynchronizationUpdateNotification> SynchronizationUpdates()
		{
			EnsureConnectionInitiated();

#pragma warning disable 4014
			var observable = subjects.GetOrAdd("sync", s => new NotificationSubject<SynchronizationUpdateNotification>(
                                                               () => ConfigureConnection("watch-sync"),
															   () => ConfigureConnection("unwatch-sync"),
															   x => true));
#pragma warning restore 4014

			return (IObservable<SynchronizationUpdateNotification>)observable;
		}

        internal IObservable<CancellationNotification> FailedUploads()
		{
			EnsureConnectionInitiated();

#pragma warning disable 4014
            var observable = subjects.GetOrAdd("cancellations", s => new NotificationSubject<CancellationNotification>(
															   () => ConfigureConnection("watch-cancellations"),
															   () => ConfigureConnection("unwatch-cancellations"),
															   x => true));
#pragma warning restore 4014

            return (IObservable<CancellationNotification>)observable;
		}

		private Task Send(string command, string value)
		{
			try
			{
				var sendUrl = url + "/changes/config?id=" + id + "&command=" + command;
				if (string.IsNullOrEmpty(value) == false)
					sendUrl += "&value=" + Uri.EscapeUriString(value);

				var request = (HttpWebRequest)WebRequest.Create(sendUrl);
				request.Method = "GET";
				return request.GetResponseAsync().ObserveException();
			}
			catch (Exception e)
			{
				return Util.TaskExtensions.FromException(e).ObserveException();
			}
		}

		public void Dispose()
		{
			if (disposed)
				return;

			DisposeAsync().Wait();
		}

		private bool disposed;

		public async Task DisposeAsync()
		{
			if (disposed)
			{
				await TaskEx.FromResult(true);
				return;
			}
			disposed = true;
			reconnectAttemptsRemaining = 0;

			if (connection == null)
			{
				await TaskEx.FromResult(true);
				return;
			}

			foreach (var subject in subjects)
			{
				subject.Value.OnCompleted();
			}

			await Send("disconnect", null);

			try
			{
				connection.Dispose();
			}
			catch (Exception)
			{
			}
		}

		public void OnNext(string dataFromConnection)
		{
			var ravenJObject = RavenJObject.Parse(dataFromConnection);
			var value = ravenJObject.Value<RavenJObject>("Value");
			var type = ravenJObject.Value<string>("Type");

			Notification notification;

			switch (type)
			{
                case "ConfigurationChangeNotification":
                    notification = JsonConvert.DeserializeObject<ConfigurationChangeNotification>(value.ToString());
					break;
                case "FileChangeNotification":
                    notification = JsonConvert.DeserializeObject<FileChangeNotification>(value.ToString());
					break;
                case "ConflictNotification":
                    notification = JsonConvert.DeserializeObject<ConflictNotification>(value.ToString());
					break;
                case "SynchronizationUpdateNotification":
                    notification = JsonConvert.DeserializeObject<SynchronizationUpdateNotification>(value.ToString());
					break;
                case "CancellationNotification":
                    notification = JsonConvert.DeserializeObject<CancellationNotification>(value.ToString());
					break;
                case "ConflictDetectedNotification":
                    notification = JsonConvert.DeserializeObject<ConflictDetectedNotification>(value.ToString());
					break;
                case "ConflictResolvedNotification":
                    notification = JsonConvert.DeserializeObject<ConflictResolvedNotification>(value.ToString());
					break;
				case "Heartbeat":
					return;
				default:
					return;
			}

			foreach (var subject in subjects)
			{
				subject.Value.OnNext(notification);
			}

		}

		public void OnError(Exception error)
		{
			if (reconnectAttemptsRemaining <= 0)
				return;

			try
			{
				EstablishConnection().ObserveException().Wait();
			}
			catch (Exception exception)
			{
				foreach (var subject in subjects)
				{
					subject.Value.OnError(exception);
				}
				subjects.Clear();
			}
		}

		public void OnCompleted()
		{
		}

		public ProfilingInformation ProfilingInformation { get; private set; }
	}
}
