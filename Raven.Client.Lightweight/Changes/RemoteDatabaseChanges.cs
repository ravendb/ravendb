using System;
using System.Linq;
using System.Net;
using System.Security.Policy;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
#if NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Client.Changes
{
	using Raven.Abstractions.Connection;

	public class RemoteDatabaseChanges : IDatabaseChanges, IDisposable, IObserver<string>
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();
		private readonly ConcurrentSet<string> watchedDocs = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedPrefixes = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedTypes = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedCollections = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedIndexes = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedBulkInserts = new ConcurrentSet<string>();
		private bool watchAllDocs;
		private bool watchAllIndexes;
#if !NETFX_CORE
		private Timer clientSideHeartbeatTimer;
#endif
		private readonly string url;
        private readonly OperationCredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly DocumentConvention conventions;
		private readonly IDocumentStoreReplicationInformer replicationInformer;
		private readonly Action onDispose;
        private readonly Func<string, Etag, string[], OperationMetadata, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync;
		private readonly AtomicDictionary<LocalConnectionState> counters = new AtomicDictionary<LocalConnectionState>(StringComparer.OrdinalIgnoreCase);
		private IDisposable connection;
		private DateTime lastHeartbeat;

		private static int connectionCounter;
		private readonly string id;

		public RemoteDatabaseChanges(
			string url,
			string apiKey,
			ICredentials credentials,
			HttpJsonRequestFactory jsonRequestFactory,
			DocumentConvention conventions,
			IDocumentStoreReplicationInformer replicationInformer,
			Action onDispose,
            Func<string, Etag, string[], OperationMetadata, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync)
		{
			ConnectionStatusChanged = LogOnConnectionStatusChanged;
			id = Interlocked.Increment(ref connectionCounter) + "/" +
				 Base62Util.Base62Random();
			this.url = url;
            this.credentials = new OperationCredentials(apiKey, credentials);
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
					return (IDatabaseChanges)this;
				});
		}

		private async Task EstablishConnection()
		{
			if (disposed)
				return;

#if !NETFX_CORE
			if (clientSideHeartbeatTimer != null)
			{
				clientSideHeartbeatTimer.Dispose();
				clientSideHeartbeatTimer = null;
			}
#endif

			var requestParams = new CreateHttpJsonRequestParams(null, url + "/changes/events?id=" + id, "GET", credentials,
																conventions)
			{
				AvoidCachingRequest = true,
				DisableRequestCompression = true
			};

			logger.Info("Trying to connect to {0} with id {1}", requestParams.Url, id);
			bool retry = false;
			IObservable<string> serverEvents = null;
			try
			{
                serverEvents = await jsonRequestFactory.CreateHttpJsonRequest(requestParams).ServerPullAsync().ConfigureAwait(false);
			}
			catch (Exception e)
			{
				logger.WarnException("Could not connect to server: " + url + " and id " + id, e);
				Connected = false;
				ConnectionStatusChanged(this, EventArgs.Empty);

				if (disposed)
					throw;

				bool timeout;
				if (replicationInformer.IsServerDown(e, out timeout) == false)
					throw;

				if (replicationInformer.IsHttpStatus(e, HttpStatusCode.NotFound, HttpStatusCode.Forbidden, HttpStatusCode.ServiceUnavailable))
					throw;

				logger.Warn("Failed to connect to {0} with id {1}, will try again in 15 seconds", url, id);
				retry = true;
			}

			if (retry)
			{
                await Time.Delay(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                await EstablishConnection().ConfigureAwait(false);
				return;
			}
			if (disposed)
			{
				Connected = false;
				ConnectionStatusChanged(this, EventArgs.Empty);
				throw new ObjectDisposedException("RemoteDatabaseChanges");
			}

			Connected = true;
			ConnectionStatusChanged(this, EventArgs.Empty);
			connection = (IDisposable)serverEvents;
			serverEvents.Subscribe(this);
			
#if !NETFX_CORE
			clientSideHeartbeatTimer = new Timer(ClientSideHeartbeat, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
#endif

			if (watchAllDocs)
                await Send("watch-docs", null).ConfigureAwait(false);

			if (watchAllIndexes)
                await Send("watch-indexes", null).ConfigureAwait(false);

			foreach (var watchedDoc in watchedDocs)
			{
                await Send("watch-doc", watchedDoc).ConfigureAwait(false);
			}

			foreach (var watchedPrefix in watchedPrefixes)
			{
                await Send("watch-prefix", watchedPrefix).ConfigureAwait(false);
			}

			foreach (var watchedCollection in watchedCollections)
			{
				await Send("watch-collection", watchedCollection);
			}

			foreach (var watchedType in watchedTypes)
			{
				await Send("watch-type", watchedType);
			}

			foreach (var watchedIndex in watchedIndexes)
			{
                await Send("watch-indexes", watchedIndex).ConfigureAwait(false);
			}

			foreach (var watchedBulkInsert in watchedBulkInserts)
			{
                await Send("watch-bulk-operation", watchedBulkInsert).ConfigureAwait(false);
			}

		}

		private void ClientSideHeartbeat(object _)
		{
			TimeSpan elapsedTimeSinceHeartbeat = SystemTime.UtcNow - lastHeartbeat;
			if (elapsedTimeSinceHeartbeat.TotalSeconds < 45)
				return;
			OnError(new TimeoutException("Over 45 seconds have passed since we got a server heartbeat, even though we should get one every 10 seconds or so.\r\n" +
										 "This connection is now presumed dead, and will attempt reconnection"));
		}

		public bool Connected { get; private set; }
		public event EventHandler ConnectionStatusChanged;

		private void LogOnConnectionStatusChanged(object sender, EventArgs eventArgs)
		{
			logger.Info("Connection ({1}) status changed, new status: {0}", Connected, url);
		}

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
				logger.Info("Sending command {0} - {1} to {2} with id {3}", command, value, url, id);
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

		public IObservableWithTask<BulkInsertChangeNotification> ForBulkInsert(Guid operationId)
		{
			var id = operationId.ToString();

			var counter = counters.GetOrAdd("bulk-operations/" + id, s =>
			{
                watchedBulkInserts.TryAdd(id);
				var documentSubscriptionTask = AfterConnection(() =>
				{
                    if (watchedBulkInserts.Contains(id)) // might have been removed in the meantime
                        return Send("watch-bulk-operation", id);
                    return Task;
				});

				return new LocalConnectionState(
					() =>
					{
						watchedBulkInserts.TryRemove(id);
						Send("unwatch-bulk-operation", id);
						counters.Remove("bulk-operations/" + operationId);
					},
					documentSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<BulkInsertChangeNotification>(counter,
																					  notification =>
																					  notification.OperationId == operationId);

			counter.OnBulkInsertChangeNotification += taskedObservable.Send;
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
				notification => notification.Id != null && notification.Id.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

			counter.OnDocumentChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection(string collectionName)
		{
			if (collectionName == null) throw new ArgumentNullException("collectionName");

			var counter = counters.GetOrAdd("collections/" + collectionName, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
				{
					watchedCollections.TryAdd(collectionName);
					return Send("watch-collection", collectionName);
				});

				return new LocalConnectionState(
					() =>
					{
						watchedCollections.TryRemove(collectionName);
						Send("unwatch-collection", collectionName);
						counters.Remove("collections/" + collectionName);
					},
					documentSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<DocumentChangeNotification>(
				counter,
				notification => string.Equals(collectionName, notification.CollectionName, StringComparison.OrdinalIgnoreCase));

			counter.OnDocumentChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection<TEntity>()
		{
			var collectionName = conventions.GetTypeTagName(typeof(TEntity));
			return ForDocumentsInCollection(collectionName);
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(string typeName)
		{
			if (typeName == null) throw new ArgumentNullException("typeName");
			var encodedTypeName = Uri.EscapeDataString(typeName);

			var counter = counters.GetOrAdd("types/" + typeName, s =>
			{
				var documentSubscriptionTask = AfterConnection(() =>
				{
					watchedTypes.TryAdd(typeName);
					return Send("watch-type", encodedTypeName);
				});

				return new LocalConnectionState(
					() =>
					{
						watchedTypes.TryRemove(typeName);
						Send("unwatch-type", encodedTypeName);
						counters.Remove("types/" + typeName);
					},
					documentSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<DocumentChangeNotification>(
				counter,
				notification => string.Equals(typeName, notification.TypeName, StringComparison.OrdinalIgnoreCase));

			counter.OnDocumentChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(Type type)
		{
			if (type == null) throw new ArgumentNullException("type");

			var typeName = ReflectionUtil.GetFullNameWithoutVersionInformation(type);
			return ForDocumentsOfType(typeName);
		}

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType<TEntity>()
		{
			var typeName = ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(TEntity));
			return ForDocumentsOfType(typeName);
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

			DisposeAsync().Wait();
		}

		private volatile bool disposed;

		public Task DisposeAsync()
		{
			if (disposed)
				return new CompletedTask();
			disposed = true;
			onDispose();

#if !NETFX_CORE
			if (clientSideHeartbeatTimer != null)
				clientSideHeartbeatTimer.Dispose();
			clientSideHeartbeatTimer = null;
#endif

			return Send("disconnect", null).
				ContinueWith(_ =>
								{
									try
									{
										if (connection != null)
											connection.Dispose();
									}
									catch (Exception e)
									{
										logger.ErrorException("Got error from server connection for " + url + " on id " + id, e);

									}
								});
		}

		public void OnNext(string dataFromConnection)
		{
			lastHeartbeat = SystemTime.UtcNow;

			var ravenJObject = RavenJObject.Parse(dataFromConnection);
			var value = ravenJObject.Value<RavenJObject>("Value");
			var type = ravenJObject.Value<string>("Type");

			logger.Debug("Got notification from {0} id {1} of type {2}", url, id, dataFromConnection);

			switch (type)
			{
				case "DocumentChangeNotification":
					var documentChangeNotification = value.JsonDeserialization<DocumentChangeNotification>();
					foreach (var counter in counters)
					{
						counter.Value.Send(documentChangeNotification);
					}
					break;

				case "BulkInsertChangeNotification":
					var bulkInsertChangeNotification = value.JsonDeserialization<BulkInsertChangeNotification>();
					foreach (var counter in counters)
					{
						counter.Value.Send(bulkInsertChangeNotification);
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
                case "Disconnect":
                    if (connection != null)
                        connection.Dispose();
                    RenewConnection();
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
			logger.ErrorException("Got error from server connection for " + url + " on id " + id, error);

            RenewConnection();
        }

        private void RenewConnection()
        {
            Time.Delay(TimeSpan.FromSeconds(15))
               .ContinueWith(_ => EstablishConnection())
               .Unwrap()
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
