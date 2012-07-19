using System;
using System.Collections.Concurrent;
using System.Net;
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
using Raven.Json.Linq;

namespace Raven.Client.Changes
{
	public class RemoteDatabaseChanges : IDatabaseChanges, IDisposable
	{
		private readonly string url;
		private readonly ICredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly DocumentConvention conventions;
		private Imports.SignalR.Client.Connection connection;
		private readonly AtomicDictionary<LocalConnectionState> counters = new AtomicDictionary<LocalConnectionState>(StringComparer.InvariantCultureIgnoreCase);

		
		[CLSCompliant(false)]
		public ConnectionState State
		{
			get { return connection.State; }
		}

		public event Action<StateChange> StateChanged
		{
			add { connection.StateChanged += value; }
			remove { connection.StateChanged -= value; }
		}

		private void ParseAndSend(string dataFromConnection)
		{
			var ravenJObject = RavenJObject.Parse(dataFromConnection);
			var value = ravenJObject.Value<RavenJObject>("Value");
			switch (ravenJObject.Value<string>("Type"))
			{
				case "DocumentChangeNotification":
					var documentChangeNotification = value.JsonDeserialization<DocumentChangeNotification>();

					foreach (var counter in counters)
					{
						counter.Value.Send(documentChangeNotification);
					}
					break;

				case "IndexChangeNotification":
					var indexChangeNotification = value.JsonDeserialization<IndexChangeNotification>();
					foreach (var counter in counters)
					{
						counter.Value.Send(indexChangeNotification);
					}break;
			}
		}

		public RemoteDatabaseChanges(string url, ICredentials credentials, HttpJsonRequestFactory jsonRequestFactory, DocumentConvention conventions)
		{
			this.url = url + (url.EndsWith("/") ? "signalr/changes" : "/signalr/changes");
			this.credentials = credentials;
			this.jsonRequestFactory = jsonRequestFactory;
			this.conventions = conventions;
			Task = EstablishConnection(0)
				.ObserveException();
		}

		private Task EstablishConnection(int retries)
		{
			var temporaryConnection = new Imports.SignalR.Client.Connection(url)
			{
				Credentials = credentials
			};
			temporaryConnection.OnPrepareRequest += jsonRequestFactory.InvokeConfigureSignalRConnection;
			return temporaryConnection.Start()
				.ContinueWith(task =>
				              	{
				              		if(task.IsFaulted == false)
										connection = temporaryConnection;
				              		return task;
				              	})
				.Unwrap()
				.ContinueWith(task =>
				              	{
				              		var webException = task.Exception.ExtractSingleInnerException() as WebException;
				              		if (webException == null || retries >= 3)
				              			return task; // effectively throw

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
				              	}).Unwrap();
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
				if(ConnectedToServer == false)
					throw new InvalidOperationException("Not connected to server");
				return action();
			})
			.Unwrap();
		}

		public IObservableWithTask<IndexChangeNotification> IndexSubscription(string indexName)
		{
			var counter = counters.GetOrAdd("indexes/"+indexName, s =>
			{
				var indexSubscriptionTask = AfterConnection(() =>
					connection.Send(new { Type = "WatchIndex", Name = indexName })).ObserveException();

				return new LocalConnectionState(
					() =>
						{
							if (ConnectedToServer)
								connection.Send(new { Type = "UnwatchIndex", Name = indexName }).ObserveException();
							counters.Remove("indexes/" + indexName);
						},
					indexSubscriptionTask);
			});
			counter.Inc();
			var taskedObservable = new TaskedObservable<IndexChangeNotification>(
				counter, 
				notification => string.Equals(notification.Name, indexName, StringComparison.InvariantCultureIgnoreCase));

			counter.OnIndexChangeNotification += taskedObservable.Send;

			var disposableTask = counter.Task.ContinueWith(task =>
			{
				if (task.IsFaulted)
					return null;
				connection.Received += ParseAndSend;
				return (IDisposable) new DisposableAction(() => connection.Stop());
			});

			counter.Add(disposableTask);
			return taskedObservable;

		}

		protected bool ConnectedToServer
		{
			get { return connection != null && connection.State != ConnectionState.Disconnected; }
		}

		public IObservableWithTask<DocumentChangeNotification> DocumentSubscription(string docId)
		{
			var counter = counters.GetOrAdd("docs/" + docId, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
						connection.Send(new { Type = "WatchDocument", Name = docId })).ObserveException();
				return new LocalConnectionState(
					() =>
						{
							if (ConnectedToServer)
								connection.Send(new { Type = "UnwatchDocument", Name = docId }).ObserveException();
							counters.Remove("docs/" + docId);
						},
					documentSubscriptionTask);
			});
			var taskedObservable = new TaskedObservable<DocumentChangeNotification>(
				counter, 
				notification => string.Equals(notification.Name, docId, StringComparison.InvariantCultureIgnoreCase));

			counter.OnDocumentChangeNotification += taskedObservable.Send;
			
			var disposableTask = counter.Task.ContinueWith(task =>
			{
				if (task.IsFaulted)
					return null;
				connection.Received += ParseAndSend;
				return (IDisposable)new DisposableAction(() => connection.Stop());
			});
			counter.Add(disposableTask);
			return taskedObservable;
		}

		public IObservableWithTask<DocumentChangeNotification> DocumentPrefixSubscription(string docIdPrefix)
		{
			var counter = counters.GetOrAdd("prefixes/" + docIdPrefix, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
						connection.Send(new { Type = "WatchDocumentPrefix", Name = docIdPrefix })).ObserveException();
				return new LocalConnectionState(
					() =>
					{
						if (connection.State != ConnectionState.Disconnected)
							connection.Send(new { Type = "UnwatchDocumentPrefix", Name = docIdPrefix }).ObserveException();
						counters.Remove("prefixes/" + docIdPrefix);
					},
					documentSubscriptionTask);
			});
			var taskedObservable = new TaskedObservable<DocumentChangeNotification>(
				counter,
				notification => notification.Name.StartsWith(docIdPrefix, StringComparison.InvariantCultureIgnoreCase));

			counter.OnDocumentChangeNotification += taskedObservable.Send;

			var disposableTask = counter.Task.ContinueWith(task =>
			{
				if (task.IsFaulted)
					return null;
				connection.Received += ParseAndSend;
				return (IDisposable)new DisposableAction(() => connection.Stop());
			});
			counter.Add(disposableTask);
			return taskedObservable;
		}

		public void Dispose()
		{
			foreach (var keyValuePair in counters)
			{
				keyValuePair.Value.Dispose();
			}
			if(connection != null)
			{
				connection.Stop();
			}
		}
	}
}