using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#endif
using Raven.Imports.SignalR.Client;
using Raven.Imports.SignalR.Client.Hubs;
using Raven.Client.Extensions;

namespace Raven.Client.Changes
{
	public class RemoteDatabaseChanges : IDatabaseChanges
	{
		private readonly string url;
		private readonly ICredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly DocumentConvention conventions;
		private HubConnection hubConnection;
		private IHubProxy proxy;
		private readonly AtomicDictionary<Counter> counters = new AtomicDictionary<Counter>(StringComparer.InvariantCultureIgnoreCase);

		private class Counter
		{
			private readonly Action onZero;
			private readonly Task task;
			private int value;

			public Task Task
			{
				get { return task; }
			}

			public Counter(Action onZero, Task task)
			{
				this.onZero = onZero;
				this.task = task;
			}

			public void Inc()
			{
				Interlocked.Increment(ref value);
			}

			public void Dec()
			{
				if (Interlocked.Decrement(ref value) == 0)
					onZero();
			}
		}
		
		[CLSCompliant(false)]
		public ConnectionState State
		{
			get { return hubConnection.State; }
		}

		public event Action<StateChange> StateChanged
		{
			add { hubConnection.StateChanged += value; }
			remove { hubConnection.StateChanged -= value; }
		}

		public RemoteDatabaseChanges(string url, ICredentials credentials, HttpJsonRequestFactory jsonRequestFactory, DocumentConvention conventions)
		{
			this.url = url;
			this.credentials = credentials;
			this.jsonRequestFactory = jsonRequestFactory;
			this.conventions = conventions;
			Task = EstablishConnection(0);
		}

		private Task EstablishConnection(int retries)
		{
			var temporaryConnection = new HubConnection(url)
			{
				Credentials = credentials
			};
			temporaryConnection.OnPrepareRequest += jsonRequestFactory.InvokeConfigureSignalRConnection;
			temporaryConnection.CreateProxy("NotificationsHub");
			return temporaryConnection.Start()
				.ContinueWith(task =>
				{
					var webException = task.Exception.ExtractSingleInnerException() as WebException;
					if (webException == null || retries >= 3)
						return task;// effectively throw

					var httpWebResponse = webException.Response as HttpWebResponse;
					if (httpWebResponse == null ||
						httpWebResponse.StatusCode != HttpStatusCode.Unauthorized)
						return task; // effectively throw

					var authorizeResponse = HandleUnauthorizedResponseAsync(httpWebResponse);

					if (authorizeResponse == null)
						return task; // effectively throw

					return authorizeResponse
						.ContinueWith(_ =>
						{
							_.Wait(); //throw on error
							return EstablishConnection(retries + 1);
						})
						.Unwrap();
				}).Unwrap()
				.ContinueWith(task =>
				{
					task.AssertNotFailed();

					hubConnection = temporaryConnection;
					proxy = hubConnection.CreateProxy("NotificationsHub");
				});
		}

		public Task HandleUnauthorizedResponseAsync(HttpWebResponse unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return null;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);

			if (unauthorizedResponseAsync == null)
				return null;

			return unauthorizedResponseAsync;
		}

		public Task Task { get; private set; }

		private Task AfterConnection(Func<Task> action)
		{
			return Task.ContinueWith(task =>
			{
				task.AssertNotFailed();
				return action();
			})
			.Unwrap();
		}

		public IObservableWithTask<IndexChangeNotification> IndexSubscription(string indexName)
		{
			var counter = counters.GetOrAdd("indexes/"+indexName, s =>
			{
				var indexSubscriptionTask = AfterConnection(() => proxy.Invoke("StartWatchingIndex", indexName));

				return new Counter(
					() =>
					{
						proxy.Invoke("StopWatchingIndex", indexName);
						counters.Remove("indexes/"+indexName);
					},
					indexSubscriptionTask);
			});
			counter.Inc();
			var taskedObservable = new TaskedObservable<IndexChangeNotification>(
				counter.Task, 
				notification => string.Equals(notification.Name, indexName, StringComparison.InvariantCultureIgnoreCase),
				counter.Dec);

			counter.Task.ContinueWith(task =>
			{
				if (task.IsFaulted)
					return;
				proxy.On<IndexChangeNotification>("Index", notification =>
				{
					if (string.Equals(notification.Name, indexName, StringComparison.InvariantCultureIgnoreCase) == false)
						return;
					taskedObservable.Send(notification);
				});
			});

			return taskedObservable;

		}

		public IObservableWithTask<DocumentChangeNotification> DocumentSubscription(string docId)
		{
			var counter = counters.GetOrAdd("docs/" + docId, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
						proxy.Invoke("StartWatchingDocument", docId));
				return new Counter(
					() =>
					{
						proxy.Invoke("StopWatchingDocument", docId);
						counters.Remove("docs/"+docId);
					},
					documentSubscriptionTask);
			});
			var taskedObservable = new TaskedObservable<DocumentChangeNotification>(
				counter.Task, 
				notification => string.Equals(notification.Name, docId, StringComparison.InvariantCultureIgnoreCase),
				counter.Dec);

			counter.Task.ContinueWith(task =>
			{
				if (task.IsFaulted)
					return;
				proxy.On<DocumentChangeNotification>("Document", notification =>
				{
					if (string.Equals(notification.Name, docId, StringComparison.InvariantCultureIgnoreCase) == false)
						return;
					taskedObservable.Send(notification);
				});
			});
			return taskedObservable;
		}

		public IObservableWithTask<DocumentChangeNotification> DocumentPrefixSubscription(string docIdPrefix)
		{
			return DocumentSubscription(docIdPrefix);
		}
	}
}