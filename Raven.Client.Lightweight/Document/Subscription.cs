// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Implementation;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class Subscription<T> : IObservable<T>, IDisposableAsync, IDisposable where T : class 
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private readonly AutoResetEvent newDocuments = new AutoResetEvent(false);
		private readonly ManualResetEvent anySubscriber = new ManualResetEvent(false);
		private readonly IAsyncDatabaseCommands commands;
		private readonly IDatabaseChanges changes;
		private readonly DocumentConvention conventions;
		private readonly Func<Task> ensureOpenSubscription;
		private readonly ConcurrentSet<IObserver<T>> subscribers = new ConcurrentSet<IObserver<T>>();
		private readonly SubscriptionConnectionOptions options;
		private readonly CancellationTokenSource cts = new CancellationTokenSource();
		private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
		private readonly bool isStronglyTyped;
		private bool completed;
		private readonly long id;
		private bool disposed;
		private bool firstConnection = true;

		public event Action BeforeBatch = delegate { };
		public event Action AfterBatch = delegate { };

		internal Subscription(long id, string database, SubscriptionConnectionOptions options, IAsyncDatabaseCommands commands, IDatabaseChanges changes, DocumentConvention conventions, Func<Task> ensureOpenSubscription)
		{
			this.id = id;
			this.options = options;
			this.commands = commands;
			this.changes = changes;
			this.conventions = conventions;
			this.ensureOpenSubscription = ensureOpenSubscription;

			if (typeof (T) != typeof (RavenJObject))
			{
				isStronglyTyped = true;
				generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions, entity => conventions.GenerateDocumentKeyAsync(database, commands, entity).ResultUnwrap());
			}

			StartWatchingDocs();
			StartPullingTask = StartPullingDocs();
		}

		public Task PullingTask { get; private set; }
		public Task StartPullingTask { get; private set; }
		private IDisposable PutDocumentsObserver { get; set; }
		private IDisposable EndedBulkInsertsObserver { get; set; }

		private Task PullDocuments()
		{
			return Task.Run(async () =>
			{
				while (true)
				{
					anySubscriber.WaitOne();

					cts.Token.ThrowIfCancellationRequested();

					var pulledDocs = false;
					Etag lastProcessedEtagOnServer = null;

					using (var subscriptionRequest = CreatePullingRequest())
					using (var response = await subscriptionRequest.ExecuteRawResponseAsync().ConfigureAwait(false))
					{
						await response.AssertNotFailingResponse().ConfigureAwait(false);

						using (var responseStream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
						{
							cts.Token.ThrowIfCancellationRequested();

							using (var streamedDocs = new AsyncServerClient.YieldStreamResults(subscriptionRequest, responseStream, customizedEndResult: reader =>
							{
								if (Equals("LastProcessedEtag", reader.Value) == false)
									return false;

								lastProcessedEtagOnServer = Etag.Parse(reader.ReadAsString().ResultUnwrap());
								return true;
							}))
							{
								while (await streamedDocs.MoveNextAsync().ConfigureAwait(false))
								{
									if (pulledDocs == false) // first doc in batch
										BeforeBatch();

									pulledDocs = true;

									cts.Token.ThrowIfCancellationRequested();

									var jsonDoc = streamedDocs.Current;

									T instance = null;

									foreach (var subscriber in subscribers)
									{
										try
										{
											if (isStronglyTyped)
											{
												if (instance == null)
												{
													instance = jsonDoc.Deserialize<T>(conventions);

													var docId = jsonDoc[Constants.Metadata].Value<string>("@id");

													if (string.IsNullOrEmpty(docId) == false)
														generateEntityIdOnTheClient.TrySetIdentity(instance, docId);
												}
												
												subscriber.OnNext(instance);
											}
											else
											{
												subscriber.OnNext((T) (object) jsonDoc);
											}
										}
										catch (Exception ex)
										{
											logger.WarnException("Subscriber threw an exception", ex);

											if (options.IgnoreSubscribersErrors == false)
											{
												IsErrored = true;

												try
												{
													subscriber.OnError(ex);
												}
												catch (Exception)
												{
													// can happen if a subscriber doesn't have an onError handler - just ignore it
												}
												break;
											}
										}
									}

									if (IsErrored)
										break;
								}
							}
						}
					}

					if (IsErrored)
						break;

					if (pulledDocs)
					{
						using (var acknowledgmentRequest = CreateAcknowledgmentRequest(lastProcessedEtagOnServer))
						{
							try
							{
								acknowledgmentRequest.ExecuteRequest();
							}
							catch (Exception)
							{
								if (acknowledgmentRequest.ResponseStatusCode != HttpStatusCode.RequestTimeout) // ignore acknowledgment timeouts
									throw;
							}
						}

						AfterBatch();

						continue; // try to pull more documents from subscription
					}

					while (newDocuments.WaitOne(options.ClientAliveNotificationInterval) == false)
					{
						using (var clientAliveRequest = CreateClientAliveRequest())
						{
							clientAliveRequest.ExecuteRequest();
						}
					}
				}
			});
		}


		/// <summary>
		/// It indicates if the subscription is in errored state.
		/// </summary>
		public bool IsErrored { get; private set; }

		/// <summary>
		/// It determines if the subscription is closed.
		/// </summary>
		public bool IsClosed { get; private set; }

		private async Task StartPullingDocs()
		{
			PullingTask = PullDocuments().ObserveException();

			try
			{
				await PullingTask.ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (cts.Token.IsCancellationRequested)
					return;

				PullingTask = null;

				SubscriptionException subscriptionEx;
				var ere = ex as ErrorResponseException;

				if (ere != null && AsyncDocumentSubscriptions.TryGetSubscriptionException(ere, out subscriptionEx))
				{
					if (subscriptionEx is SubscriptionClosedException)
					{
						// someone forced us to drop the connection by calling Subscriptions.Release
						OnCompletedNotification();
						IsClosed = true;
						return;
					}
				}

				RestartPullingTask().ConfigureAwait(false);
			}

			if (IsErrored)
			{
				OnCompletedNotification();

				try
				{
					await CloseSubscription().ConfigureAwait(false);
				}
				catch (Exception e)
				{
					logger.WarnException("Exception happened during an attempt to close subscription after it becomes faulted", e);
				}
			}
		}

		private async Task RestartPullingTask()
		{
			await Time.Delay(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
			try
			{
				await ensureOpenSubscription().ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (ex is SubscriptionInUseException || ex is SubscriptionDoesNotExistExeption)
				{
					// another client has connected to the subscription or it has been deleted meanwhile - we cannot open it so we need to finish
					OnCompletedNotification();
					return;
				}

				RestartPullingTask().ConfigureAwait(false);
				return;
			}

			StartPullingTask = StartPullingDocs().ObserveException();
		}

		private void StartWatchingDocs()
		{
			changes.ConnectionStatusChanged += ChangesApiConnectionChanged;

			PutDocumentsObserver = changes.ForAllDocuments().Subscribe(notification =>
			{
				if (notification.Type == DocumentChangeTypes.Put && notification.Id.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase) == false)
				{
					newDocuments.Set();
				}
			});

			EndedBulkInsertsObserver = changes.ForBulkInsert().Subscribe(notification =>
			{
				if (notification.Type == DocumentChangeTypes.BulkInsertEnded)
				{
					newDocuments.Set();
				}
			});
		}

		private void ChangesApiConnectionChanged(object sender, EventArgs e)
		{
			if (firstConnection)
			{
				firstConnection = false;
				return;
			}

			var changesApi = (RemoteDatabaseChanges) sender;

			if (changesApi.Connected)
				newDocuments.Set();
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			if(IsErrored)
				throw new InvalidOperationException("Subscription encountered errors and stopped. Cannot add any subscriber.");

			if (subscribers.TryAdd(observer))
			{
				if (subscribers.Count == 1)
					anySubscriber.Set();
			}

			return new DisposableAction(() =>
			{
				subscribers.TryRemove(observer);
				if (subscribers.Count == 0)
					anySubscriber.Reset();
			});
		}

		private HttpJsonRequest CreateAcknowledgmentRequest(Etag lastProcessedEtag)
		{
			return commands.CreateRequest(string.Format("/subscriptions/acknowledgeBatch?id={0}&lastEtag={1}&connection={2}", id, lastProcessedEtag, options.ConnectionId), HttpMethods.Post);
		}

		private HttpJsonRequest CreatePullingRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/pull?id={0}&connection={1}", id, options.ConnectionId), HttpMethods.Get);
		}

		private HttpJsonRequest CreateClientAliveRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/client-alive?id={0}&connection={1}", id, options.ConnectionId), HttpMethods.Patch);
		}

		private HttpJsonRequest CreateCloseRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/close?id={0}&connection={1}", id, options.ConnectionId), HttpMethods.Post);
		}

		private void OnCompletedNotification()
		{
			if(completed)
				return;

			foreach (var subscriber in subscribers)
			{
				subscriber.OnCompleted();
			}

			completed = true;
		}

		public void Dispose()
		{
			if (disposed)
				return;

			DisposeAsync().Wait();
		}

		public Task DisposeAsync()
		{
			if (disposed)
				return new CompletedTask();

			disposed = true;

			OnCompletedNotification();

			subscribers.Clear();

			if (PutDocumentsObserver != null)
				PutDocumentsObserver.Dispose();

			if(EndedBulkInsertsObserver != null)
				EndedBulkInsertsObserver.Dispose();

			var disposableChanges = changes as IDisposable;

			if (disposableChanges != null)
				disposableChanges.Dispose();

			cts.Cancel();

			newDocuments.Set();
			anySubscriber.Set();

			changes.ConnectionStatusChanged -= ChangesApiConnectionChanged;

			foreach (var task in new []{PullingTask, StartPullingTask})
			{
				if (task == null) 
					continue;
				
				switch (task.Status)
				{
					case TaskStatus.RanToCompletion:
					case TaskStatus.Canceled:
						break;
					default:
						try
						{
							task.Wait();
						}
						catch (AggregateException ae)
						{
							if (ae.InnerException is OperationCanceledException == false)
							{
								throw;
							}
						}

						break;
				}
			}

			if (IsClosed)
				return new CompletedTask();

			return CloseSubscription();
		}

		private async Task CloseSubscription()
		{
			using (var closeRequest = CreateCloseRequest())
			{
				await closeRequest.ExecuteRequestAsync().ConfigureAwait(false);
				IsClosed = true;
			}
		}
	}
}