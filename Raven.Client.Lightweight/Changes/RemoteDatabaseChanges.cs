using System;
using System.Linq;
using System.Net;
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
#elif NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Client.Changes
{
	public class RemoteDatabaseChanges : IDatabaseChanges, IDisposable, IObserver<string>
	{
		private readonly ILog logger = LogManager.GetCurrentClassLogger();
		private readonly ConcurrentSet<string> watchedDocs = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedPrefixes = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedIndexes = new ConcurrentSet<string>();
		private bool watchAllDocs;
		private bool watchAllIndexes;

		private readonly string url;
		private readonly ICredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly DocumentConvention conventions;
		private readonly ReplicationInformer replicationInformer;
		private readonly Action onDispose;
		private readonly Func<string, Etag, string[], string, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync;
		private readonly AtomicDictionary<LocalConnectionState> counters = new AtomicDictionary<LocalConnectionState>(StringComparer.OrdinalIgnoreCase);
		private IDisposable connection;

		private static int connectionCounter;
		private readonly string id;

		public RemoteDatabaseChanges(string url, ICredentials credentials, HttpJsonRequestFactory jsonRequestFactory,
		                             DocumentConvention conventions, ReplicationInformer replicationInformer, Action onDispose,
		                             Func<string, Etag, string[], string, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync)
		{
			id = Interlocked.Increment(ref connectionCounter) + "/" +
			     Base62Util.Base62Random();
			this.url = url;
			this.credentials = credentials;
			this.jsonRequestFactory = jsonRequestFactory;
			this.conventions = conventions;
			this.replicationInformer = replicationInformer;
			this.onDispose = onDispose;
			this.tryResolveConflictByUsingRegisteredConflictListenersAsync = tryResolveConflictByUsingRegisteredConflictListenersAsync;
			Task = EstablishConnection()
				.ObserveException()
				.ContinueWith(task =>
				{
					task.AssertNotFailed();
					return (IDatabaseChanges) this;
				});
		}

		private Task EstablishConnection()
		{
			if (disposed)
				return new CompletedTask();

			var requestParams = new CreateHttpJsonRequestParams(null, url + "/changes/events?id=" + id, "GET", credentials, conventions)
									{
										AvoidCachingRequest = true
									};

			return jsonRequestFactory.CreateHttpJsonRequest(requestParams)
				.ServerPullAsync()
				.ContinueWith(task =>
								{
									if(disposed)
										throw new ObjectDisposedException("RemoteDatabaseChanges");
									if (task.IsFaulted)
									{
										logger.WarnException("Could not connect to server, will retry", task.Exception);
										Connected = false;
										ConnectionStatusChanged(this, EventArgs.Empty);
										
										if (disposed)
											return task;

										bool timeout;
										if (replicationInformer.IsServerDown(task.Exception, out timeout) == false)
											return task;

										if(replicationInformer.IsHttpStatus(task.Exception, 
												HttpStatusCode.NotFound, 
												HttpStatusCode.Forbidden))
											return task;

										return Time.Delay(TimeSpan.FromSeconds(15))
											.ContinueWith(_ => EstablishConnection())
											.Unwrap();
									}

									Connected = true;
									ConnectionStatusChanged(this, EventArgs.Empty);
									connection = (IDisposable)task.Result;
									task.Result.Subscribe(this);

									Task prev = watchAllDocs ? Send("watch-docs", null) : new CompletedTask();

									if (watchAllIndexes)
										prev = prev.ContinueWith(_ => Send("watch-indexes", null));

									prev = watchedDocs.Aggregate(prev, (cur, docId) => cur.ContinueWith(task1 => Send("watch-doc", docId)));

									prev = watchedPrefixes.Aggregate(prev, (cur, prefix) => cur.ContinueWith(task1 => Send("watch-prefix", prefix)));

									prev = watchedIndexes.Aggregate(prev, (cur, index) => cur.ContinueWith(task1 => Send("watch-indexes", index)));
								
									return prev;
								})
				.Unwrap();
		}

		public bool Connected { get; private set; }
		public event EventHandler ConnectionStatusChanged = delegate { }; 
		public Task<IDatabaseChanges> Task { get; private set; }

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
			var counter = counters.GetOrAdd("indexes/" + indexName, s =>
			{
				var indexSubscriptionTask = AfterConnection(() =>
				{
					watchedIndexes.TryAdd(indexName);
					return Send("watch-index", indexName);
				});

				return new LocalConnectionState(
					() =>
					{
						watchedIndexes.TryRemove(indexName);
						Send("unwatch-index", indexName);
						counters.Remove("indexes/" + indexName);
					},
					indexSubscriptionTask);
			});
			counter.Inc();
			var taskedObservable = new TaskedObservable<IndexChangeNotification>(
				counter,
				notification => string.Equals(notification.Name, indexName, StringComparison.OrdinalIgnoreCase));

			counter.OnIndexChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

		
			return taskedObservable;

		}

		private Task lastSendTask;

		private Task Send(string command, string value)
		{
			lock (this)
			{
				var sendTask = lastSendTask;
				if (sendTask != null)
				{
					sendTask.ContinueWith(_ =>
					{
						Send(command, value);
					});
				}

				try
				{
					var sendUrl = url + "/changes/config?id=" + id + "&command=" + command;
					if (string.IsNullOrEmpty(value) == false)
						sendUrl += "&value=" + Uri.EscapeUriString(value);

					sendUrl = sendUrl.NoCache();

					var requestParams = new CreateHttpJsonRequestParams(null, sendUrl, "GET", credentials, conventions)
					{
						AvoidCachingRequest = true
					};
					var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(requestParams);
					return lastSendTask =
						httpJsonRequest.ExecuteRequestAsync()
							.ObserveException()
							.ContinueWith(task => lastSendTask = null);
				}
				catch (Exception e)
				{
					return new CompletedTask(e).Task.ObserveException();
				}
			}
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocument(string docId)
		{
			var counter = counters.GetOrAdd("docs/" + docId, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
				{
					watchedDocs.TryAdd(docId);
					return Send("watch-doc", docId);
				});

				return new LocalConnectionState(
					() =>
					{
						watchedDocs.TryRemove(docId);
						Send("unwatch-doc", docId);
						counters.Remove("docs/" + docId);
					},
					documentSubscriptionTask);
			});
			var taskedObservable = new TaskedObservable<DocumentChangeNotification>(
				counter,
				notification => string.Equals(notification.Id, docId, StringComparison.OrdinalIgnoreCase));

			counter.OnDocumentChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
		}

		public IObservableWithTask<DocumentChangeNotification> ForAllDocuments()
		{
			var counter = counters.GetOrAdd("all-docs", s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
				{
					watchAllDocs = true;
					return Send("watch-docs", null);
				});
				return new LocalConnectionState(
					() =>
					{
						watchAllDocs = false;
						Send("unwatch-docs", null);
						counters.Remove("all-docs");
					},
					documentSubscriptionTask);
			});
			var taskedObservable = new TaskedObservable<DocumentChangeNotification>(
				counter,
				notification => true);

			counter.OnDocumentChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
		}

		public IObservableWithTask<IndexChangeNotification> ForAllIndexes()
		{
			var counter = counters.GetOrAdd("all-indexes", s =>
			{
				var indexSubscriptionTask = AfterConnection(() =>
				{
					watchAllIndexes = true;
					return Send("watch-indexes", null);
				});

				return new LocalConnectionState(
					() =>
					{
						watchAllIndexes = false;
						Send("unwatch-indexes", null);
						counters.Remove("all-indexes");
					},
					indexSubscriptionTask);
			});
			var taskedObservable = new TaskedObservable<IndexChangeNotification>(
				counter,
				notification => true);

			counter.OnIndexChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix)
		{
			var counter = counters.GetOrAdd("prefixes/" + docIdPrefix, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
				{
					watchedPrefixes.TryAdd(docIdPrefix);
					return Send("watch-prefix", docIdPrefix);
				});

				return new LocalConnectionState(
					() =>
					{
						watchedPrefixes.TryRemove(docIdPrefix);
						Send("unwatch-prefix", docIdPrefix);
						counters.Remove("prefixes/" + docIdPrefix);
					},
					documentSubscriptionTask);
			});
			var taskedObservable = new TaskedObservable<DocumentChangeNotification>(
				counter,
				notification => notification.Id.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

			counter.OnDocumentChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
		}

		public IObservableWithTask<ReplicationConflictNotification> ForAllReplicationConflicts()
		{
			var counter = counters.GetOrAdd("all-replication-conflicts", s =>
			{
				var indexSubscriptionTask = AfterConnection(() =>
				{
					watchAllIndexes = true;
					return Send("watch-replication-conflicts", null);
				});

				return new LocalConnectionState(
					() =>
					{
						watchAllIndexes = false;
						Send("unwatch-replication-conflicts", null);
						counters.Remove("all-replication-conflicts");
					},
					indexSubscriptionTask);
			});
			var taskedObservable = new TaskedObservable<ReplicationConflictNotification>(
				counter,
				notification => true);

			counter.OnReplicationConflictNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
		}

		public void WaitForAllPendingSubscriptions()
		{
			foreach (var kvp in counters)
			{
				kvp.Value.Task.Wait();
			}
		}


		public void Dispose()
		{
			if (disposed)
				return;

			DisposeAsync();
		}

		private volatile bool disposed;

		public Task DisposeAsync()
		{
			if (disposed)
				return new CompletedTask();
			disposed = true;
			onDispose();

			return Send("disconnect", null).
				ContinueWith(_ =>
								{
									try
									{
										connection.Dispose();
									}
									catch (Exception e)
									{
										logger.WarnException("Error when disposing of connection", e);
									}
								});
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
					} 
					break;
				case "ReplicationConflictNotification":
					var replicationConflictNotification = value.JsonDeserialization<ReplicationConflictNotification>();
					foreach (var counter in counters)
					{
						counter.Value.Send(replicationConflictNotification);
					}

					if (replicationConflictNotification.ItemType == ReplicationConflictTypes.DocumentReplicationConflict)
					{
						tryResolveConflictByUsingRegisteredConflictListenersAsync(replicationConflictNotification.Id,
						                                                     replicationConflictNotification.Etag,
						                                                     replicationConflictNotification.Conflicts, null)
							.ContinueWith(t =>
							{
								t.AssertNotFailed();

								if (t.Result)
								{
									logger.Debug("Document replication conflict for {0} was resolved by one of the registered conflict listeners",
									             replicationConflictNotification.Id);
								}
							});
					}

					break;
				case "Initialized":
				case "Heartbeat":
					break;
				default:
					break;
			}
		}

		public void OnError(Exception error)
		{
			logger.ErrorException("Got error from server connection", error);

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
