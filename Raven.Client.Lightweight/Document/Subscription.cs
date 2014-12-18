// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class Subscription : IObservable<RavenJObject>, IDisposable
	{
		private readonly AutoResetEvent newDocuments = new AutoResetEvent(false);
		private readonly ManualResetEvent anySubscriber = new ManualResetEvent(false);
		private readonly IAsyncDatabaseCommands commands;
		private readonly IDatabaseChanges changes;
		private readonly Func<Task> ensureOpenSubscription;
		private readonly ConcurrentSet<IObserver<RavenJObject>> subscribers = new ConcurrentSet<IObserver<RavenJObject>>();
		private readonly SubscriptionConnectionOptions options;
		private readonly CancellationTokenSource cts = new CancellationTokenSource();
		private readonly long id;
		private bool disposed;

		public event Action BeforeBatch = delegate { };
		public event Action AfterBatch = delegate { };

		internal Subscription(long id, SubscriptionConnectionOptions options, IAsyncDatabaseCommands commands, IDatabaseChanges changes, Func<Task> ensureOpenSubscription)
		{
			this.id = id;
			this.options = options;
			this.commands = commands;
			this.changes = changes;
			this.ensureOpenSubscription = ensureOpenSubscription;

			StartWatchingDocs();
			StartPullingDocs().ConfigureAwait(false);
		}

		public Task PullingTask { get; private set; }
		private IDisposable NewDocumentsObserver { get; set; }

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
									foreach (var subscriber in subscribers)
									{
										subscriber.OnNext(jsonDoc);
									}
								}
							}
						}
					}

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

		private async Task StartPullingDocs()
		{
			PullingTask = PullDocuments().ObserveException();

			try
			{
				await PullingTask.ConfigureAwait(false);
			}
			catch (Exception e)
			{
				if (cts.Token.IsCancellationRequested)
					return;

				PullingTask = null;

				OnErrorNotification(e);

				RestartPullingTask().ConfigureAwait(false);
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
					// another client has connected to the subscription or it has been deleted meanwhile - we cannot open it
					OnErrorNotification(ex);
					OnCompletedNotification();

					return;
				}

				RestartPullingTask().ConfigureAwait(false);
				return;
			}

			try
			{
				await StartPullingDocs().ConfigureAwait(false);
			}
			catch (Exception e)
			{
				var exception = new Exception("Could not restart pulling task.", e);

				OnErrorNotification(exception);
			}
		}

		private void StartWatchingDocs()
		{
			var observableWithTask = changes.ForAllDocuments();

			NewDocumentsObserver = observableWithTask.Subscribe(notification =>
			{
				if (notification.Type == DocumentChangeTypes.Put && notification.Id.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase) == false)
				{
					newDocuments.Set();
				}
			});
		}

		public IDisposable Subscribe(IObserver<RavenJObject> observer)
		{
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
			return commands.CreateRequest(string.Format("/subscriptions/acknowledgeBatch?id={0}&lastEtag={1}&connection={2}", id, lastProcessedEtag, options.ConnectionId), "POST");
		}

		private HttpJsonRequest CreatePullingRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/pull?id={0}&connection={1}", id, options.ConnectionId), "GET");
		}

		private HttpJsonRequest CreateClientAliveRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/client-alive?id={0}&connection={1}", id, options.ConnectionId), "PATCH");
		}

		private HttpJsonRequest CreateCloseRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/close?id={0}&connection={1}", id, options.ConnectionId), "POST");
		}

		private void OnErrorNotification(Exception exception)
		{
			foreach (var subscriber in subscribers)
			{
				subscriber.OnError(exception);
			}
		}

		private void OnCompletedNotification()
		{
			foreach (var subscriber in subscribers)
			{
				subscriber.OnCompleted();
			}
		}

		public void Dispose()
		{
			if (disposed)
				return;

			DisposeAsync().Wait();
		}

		public async Task DisposeAsync()
		{
			if (disposed)
				return;

			disposed = true;

			OnCompletedNotification();

			subscribers.Clear();

			if (NewDocumentsObserver != null)
				NewDocumentsObserver.Dispose();

			var disposableChanges = changes as IDisposable;

			if (disposableChanges != null)
				disposableChanges.Dispose();

			cts.Cancel();

			newDocuments.Set();
			anySubscriber.Set();

			if (PullingTask != null)
			{
				switch (PullingTask.Status)
				{
					case TaskStatus.RanToCompletion:
					case TaskStatus.Canceled:
						break;
					default:
						try
						{
							PullingTask.Wait();
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

			using (var closeRequest = CreateCloseRequest())
			{
				await closeRequest.ExecuteRequestAsync().ConfigureAwait(false);
			}
		}
	}
}