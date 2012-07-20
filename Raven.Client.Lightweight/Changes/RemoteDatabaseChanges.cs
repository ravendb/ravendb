using System;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#endif
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Changes
{
	public class RemoteDatabaseChanges : IDatabaseChanges, IDisposable, IObserver<string>
	{
		private readonly ILog logger = LogProvider.GetCurrentClassLogger();

		private readonly string url;
		private readonly ICredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly DocumentConvention conventions;
		private readonly AtomicDictionary<LocalConnectionState> counters = new AtomicDictionary<LocalConnectionState>(StringComparer.InvariantCultureIgnoreCase);
		private int reconnectAttemptsRemaining;
		private IDisposable connection;

		private static int connectionCounter;
		private readonly string id;

		public RemoteDatabaseChanges(string url, ICredentials credentials, HttpJsonRequestFactory jsonRequestFactory, DocumentConvention conventions)
		{
			id = Interlocked.Increment(ref connectionCounter) + "/" + Guid.NewGuid();
			this.url = url;
			this.credentials = credentials;
			this.jsonRequestFactory = jsonRequestFactory;
			this.conventions = conventions;
			Task = EstablishConnection()
				.ObserveException();
		}

		private Task EstablishConnection()
		{
			var requestParams = new CreateHttpJsonRequestParams(null, url + "/changes/events?id="+id, "GET", credentials, conventions)
			                    	{
			                    		AvoidCachingRequest = true
			                    	};
			return jsonRequestFactory.CreateHttpJsonRequest(requestParams)
				.ServerPullAsync()
				.ContinueWith(task =>
				              	{
				              		if (task.IsFaulted && reconnectAttemptsRemaining > 0)
				              		{
				              			logger.WarnException("Could not connect to server, will retry", task.Exception);

				              			reconnectAttemptsRemaining--;
				              			return EstablishConnection();
				              		}
				              		reconnectAttemptsRemaining = 3; // after the first successful try, we will retry 3 times before giving up
				              		connection = (IDisposable)task.Result;
				              		task.Result.Subscribe(this);
				              		return task;
				              	})
				.Unwrap();
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

		public IObservableWithTask<IndexChangeNotification> ForIndex(string indexName)
		{
			var counter = counters.GetOrAdd("indexes/"+indexName, s =>
			{
				var indexSubscriptionTask = AfterConnection(() =>
					Send(new { Type = "WatchIndex", Name = indexName }));

				return new LocalConnectionState(
					() =>
						{
							Send(new { Type = "UnwatchIndex", Name = indexName });
							counters.Remove("indexes/" + indexName);
						},
					indexSubscriptionTask);
			});
			counter.Inc();
			var taskedObservable = new TaskedObservable<IndexChangeNotification>(
				counter, 
				notification => string.Equals(notification.Name, indexName, StringComparison.InvariantCultureIgnoreCase));

			counter.OnIndexChangeNotification += taskedObservable.Send;
			counter.OnError = taskedObservable.Error;

			var disposableTask = counter.Task.ContinueWith(task =>
			{
				if (task.IsFaulted)
					return null;
				return (IDisposable) new DisposableAction(() =>
				                                          	{
				                                          		try
				                                          		{
				                                          			connection.Dispose();
				                                          		}
				                                          		catch (Exception)
				                                          		{
				                                          			// nothing to do here
				                                          		}
				                                          	});
			});

			counter.Add(disposableTask);
			return taskedObservable;

		}

		private Task Send(object msg)
		{
			var requestParams = new CreateHttpJsonRequestParams(null, url + "/changes/config?id="+id, "POST", credentials, conventions);
			try
			{
				var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(requestParams);
				return httpJsonRequest.ExecuteWriteAsync(JsonConvert.SerializeObject(msg)).ObserveException();
			}
			catch (Exception e)
			{
				return new CompletedTask(e).Task.ObserveException();
			}
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocument(string docId)
		{
			var counter = counters.GetOrAdd("docs/" + docId, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
						Send(new { Type = "WatchDocument", Name = docId }));
				return new LocalConnectionState(
					() =>
						{
							Send(new { Type = "UnwatchDocument", Name = docId });
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
				return (IDisposable)new DisposableAction(() => connection.Dispose());
			});
			counter.Add(disposableTask);
			return taskedObservable;
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix)
		{
			var counter = counters.GetOrAdd("prefixes/" + docIdPrefix, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
						Send(new { Type = "WatchDocumentPrefix", Name = docIdPrefix }));
				return new LocalConnectionState(
					() =>
					{
						Send(new { Type = "UnwatchDocumentPrefix", Name = docIdPrefix });
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
				return (IDisposable)new DisposableAction(() => connection.Dispose());
			});
			counter.Add(disposableTask);
			return taskedObservable;
		}


		public void Dispose()
		{
			reconnectAttemptsRemaining = 0;
			foreach (var keyValuePair in counters)
			{
				keyValuePair.Value.Dispose();
			}
			if(connection != null)
			{
				connection.Dispose();
			}
		}

		public void OnNext(string dataFromConnection)
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
					} break;
			}
		}

		public void OnError(Exception error)
		{
			logger.ErrorException("Got error from server connection", error);
			if (reconnectAttemptsRemaining <= 0)
				return;

			EstablishConnection()
				.ObserveException()
				.ContinueWith(task =>
				              	{
									if (task.IsFaulted == false)
										return;

				              		foreach (var keyValuePair in counters)
				              		{
				              			keyValuePair.Value.Error(task.Exception);
				              		}
				              		counters.Clear();
				              	});
		}

		public void OnCompleted()
		{
		}
	}
}