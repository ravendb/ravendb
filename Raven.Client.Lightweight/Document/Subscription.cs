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
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Database.Util;

namespace Raven.Client.Document
{
	public class Subscription : IObservable<JsonDocument>, IDisposable
	{
		private readonly SubscriptionBatchOptions options;
		private readonly string connectionId;
		private readonly IAsyncDatabaseCommands commands;
		private readonly IDatabaseChanges changes;
		private readonly ConcurrentSet<IObserver<JsonDocument>> subscribers = new ConcurrentSet<IObserver<JsonDocument>>();
		private readonly long id;
		private readonly CancellationTokenSource cts = new CancellationTokenSource();
		private bool pullingStarted;
		private bool disposed;

		public event Action BeforeBatch = delegate { };
		public event Action AfterBatch = delegate { };

		internal Subscription(long id, string connectionId, IAsyncDatabaseCommands commands, IDatabaseChanges changes)
		{
			this.id = id;
			this.connectionId = connectionId;
			this.commands = commands;
			this.changes = changes;
		}

		public Task PullingTask { get; private set; }
		private IDisposable NewDocumentsObserver { get; set; }

		private async Task PullDocuments()
		{
			if(NewDocumentsObserver != null)
				NewDocumentsObserver.Dispose();

			try
			{
				bool pulledDocs;

				do
				{
					pulledDocs = false;

					using (var subscriptionRequest = commands.CreateRequest(string.Format("/subscriptions/pull?id={0}&connection={1}", id, connectionId), "GET"))
					using (var response = await subscriptionRequest.ExecuteRawResponseAsync().ConfigureAwait(false))
					{
						await response.AssertNotFailingResponse().ConfigureAwait(false);

						using (var responseStream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
						{
							cts.Token.ThrowIfCancellationRequested();

							Etag lastProcessedEtagOnServer = null;

							using (var streamedDocs = new AsyncServerClient.YieldStreamResults(subscriptionRequest, responseStream, customEndOfResults: reader =>
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

									var jsonDoc = SerializationHelper.RavenJObjectToJsonDocument(streamedDocs.Current);
									foreach (var subscriber in subscribers)
									{
										subscriber.OnNext(jsonDoc);
									}
								}
							}

							if (pulledDocs)
							{
								using (var acknowledgmentRequest = commands.CreateRequest(string.Format("/subscriptions/acknowledgeBatch?id={0}&lastEtag={1}&connection={2}", id, lastProcessedEtagOnServer, connectionId), "POST"))
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
							}
						}
					}
				} while (pulledDocs);
				
			}
			catch (Exception ex)
			{
				if (ex.InnerException is OperationCanceledException == false)
				{
					foreach (var subscriber in subscribers)
					{
						subscriber.OnError(ex);
					}
				}

				throw;
			}

			PullingTask = null;

			NewDocumentsObserver = changes.ForAllDocuments().Subscribe(notification =>
			{
				if (notification.Type == DocumentChangeTypes.Put && notification.Id.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase) == false)
				{
					PullingTask = PullDocuments();
				}
			});
		}

		public IDisposable Subscribe(IObserver<JsonDocument> observer)
		{
			if (PullingTask != null && PullingTask.IsFaulted)
				throw new InvalidOperationException("Cannot subscribe because the subscription pulling task is faulted", PullingTask.Exception);

			if (subscribers.TryAdd(observer))
			{
				if (pullingStarted == false)
				{
					PullingTask = PullDocuments();
					pullingStarted = true;
				}
			}

			return new DisposableAction(() => subscribers.TryRemove(observer));
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

			foreach (var subscriber in subscribers)
			{
				subscriber.OnCompleted();
			}

			if (NewDocumentsObserver != null)
				NewDocumentsObserver.Dispose();

			var disposableChanges = changes as IDisposable;

			if (disposableChanges != null)
				disposableChanges.Dispose();

			cts.Cancel();

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

			using (var closeRequest = commands.CreateRequest(string.Format("/subscriptions/close?id={0}&connection={1}", id, connectionId), "POST"))
			{
				await closeRequest.ExecuteRequestAsync().ConfigureAwait(false);
				pullingStarted = false;
			}
		}

	}
}